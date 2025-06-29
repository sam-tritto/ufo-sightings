----------------------------------------------------------------------------
-- Base UFO Data
----------------------------------------------------------------------------

SELECT * FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` LIMIT 1

SELECT DISTINCT shape FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` 

SELECT 
      shape AS SHAPE, 
      COUNT(*) AS SHAPE_COUNT 
FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` 
GROUP BY shape
ORDER BY COUNT(*) DESC

----------------------------------------------------------------------------
-- Base BQ Geo Data Tables
----------------------------------------------------------------------------

SELECT * FROM `bigquery-public-data.geo_us_boundaries.zip_codes` LIMIT 1
SELECT * FROM `bigquery-public-data.geo_us_boundaries.counties` LIMIT 1
SELECT * FROM `bigquery-public-data.geo_us_boundaries.metropolitan_divisions` LIMIT 1
SELECT * FROM `bigquery-public-data.geo_us_boundaries.designated_market_area` LIMIT 1

----------------------------------------------------------------------------
-- Within County
----------------------------------------------------------------------------

WITH county_cte AS (
               SELECT 
                     county_name,
                     county_geom
               FROM `bigquery-public-data.geo_us_boundaries.counties` 
               WHERE 1=1 
               AND UPPER(TRIM(county_name)) LIKE 'DUTCHESS%' 
)

SELECT 
   ufo.datetime  AS ufo_datetime, 
   ufo.year  AS ufo_year, 
   ufo.city,
   c.county_name as county_name,
   ufo.state,
   ufo.shape,
   ufo.duration_seconds,
   ufo.comments 
FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` AS ufo
      ,county_cte AS c
WHERE 1=1
AND ST_WITHIN(
               ST_GEOGPOINT(ufo.longitude, ufo.latitude), 
               c.county_geom
               )
ORDER BY ufo.datetime DESC

----------------------------------------------------------------------------
-- Within Zip Code
----------------------------------------------------------------------------

WITH zip_cte AS (
               SELECT 
                     zip_code,
                     zip_code_geom
               FROM `bigquery-public-data.geo_us_boundaries.zip_codes` 
               WHERE 1=1 
               AND LEFT(TRIM(zip_code), 5) LIKE '12590%' 
            )
SELECT 
   ufo.datetime  AS ufo_datetime, 
   ufo.year  AS ufo_year, 
   ufo.city,
   ufo.state,
   z.zip_code as zip_code,
   ufo.shape,
   ufo.duration_seconds,
   ufo.comments 
FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` AS ufo
      ,zip_cte AS z
WHERE 1=1
AND ST_WITHIN(
               ST_GEOGPOINT(ufo.longitude, ufo.latitude), 
               z.zip_code_geom
               )
ORDER BY ufo.datetime DESC

----------------------------------------------------------------------------
-- Distance Between
----------------------------------------------------------------------------

WITH dutchess_county_cte AS (
            SELECT 
                  county_name,
                  int_point_lat,
                  int_point_lon,
            FROM `bigquery-public-data.geo_us_boundaries.counties` 
            WHERE 1=1 
            AND UPPER(TRIM(county_name)) LIKE 'DUTCHESS%' 
),
other_counties_cte AS (
            SELECT 
                  county_name,
                  county_geom,
                  int_point_lat,
                  int_point_lon,
            FROM `bigquery-public-data.geo_us_boundaries.counties` 
            WHERE 1=1 
            AND UPPER(TRIM(county_name)) NOT LIKE 'DUTCHESS%' 
),
other_county_ufos_cte AS (
            SELECT 
                  ufo.datetime AS ufo_datetime, 
                  ufo.year AS ufo_year, 
                  ufo.city,
                  c.county_name,
                  c.int_point_lat,
                  c.int_point_lon,
                  ufo.state,
                  ufo.shape,
                  ufo.duration_seconds,
                  ufo.comments 
            FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` AS ufo
                  ,other_counties_cte AS c
            WHERE 1=1
            AND ST_WITHIN(
                        ST_GEOGPOINT(ufo.longitude, ufo.latitude), 
                        c.county_geom
                        )
            AND ufo.shape IN ('chevron')
            ORDER BY ufo.datetime DESC
),
distance_cte AS (
                  SELECT 
                        ST_DISTANCE(
                              ST_GEOGPOINT(a.int_point_lon, a.int_point_lat), 
                              ST_GEOGPOINT(b.int_point_lon, b.int_point_lat)
                              ) / 1609.344 AS distance_miles,
                        b.ufo_datetime, 
                        b.ufo_year, 
                        b.city,
                        b.county_name,
                        b.state,
                        b.shape,
                        b.duration_seconds,
                        b.comments 
                  FROM dutchess_county_cte AS a
                        ,other_county_ufos_cte AS b
                  WHERE 1=1
)

SELECT * FROM distance_cte
ORDER BY distance_miles ASC


----------------------------------------------------------------------------
-- Clustering
----------------------------------------------------------------------------

WITH county_cte AS (
               SELECT 
                     county_name,
                     int_point_lat AS county_lat,
                     int_point_lon AS county_lon,
                     county_geom
               FROM `bigquery-public-data.geo_us_boundaries.counties` 
               WHERE 1=1  
),
county_ufo_cte AS (
                  SELECT 
                        ufo.datetime  AS ufo_datetime, 
                        ufo.year  AS ufo_year, 
                        ufo.city,
                        c.county_name,
                        c.county_lat,
                        c.county_lon,
                        c.county_geom,
                        ufo.state,
                        ufo.shape,
                        ufo.duration_seconds,
                        ufo.comments 
                  FROM `bigquerytestproject-343401.UFO_PROXIMITY.UFO_PROXIMITY_DATA` AS ufo
                        ,county_cte AS c
                  WHERE 1=1
                  AND ST_WITHIN(
                              ST_GEOGPOINT(ufo.longitude, ufo.latitude), 
                              c.county_geom
                              )
                  AND ufo.shape IN ('chevron')
),
cluster_cte AS (
                  SELECT 
                        ST_CLUSTERDBSCAN(county_geom, 64373.76, 3)
                              OVER () AS cluster_num,
                        ufo_datetime, 
                        ufo_year, 
                        city,
                        county_name,
                        county_lat,
                        county_lon,
                        state,
                        shape,
                        duration_seconds,
                        comments 
                  FROM county_ufo_cte
                  WHERE 1=1
)
SELECT * FROM cluster_cte
ORDER BY cluster_num ASC, ufo_year DESC

