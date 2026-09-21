-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SET enable_analyzer = 1;

SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, 42) SETTINGS transform_null_in = 1;

SELECT tuple(1, 2)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;

SELECT CAST((1, 2) AS Tuple(UInt32, UInt32)) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;
SELECT CAST((3, 4) AS Tuple(UInt32, UInt32)) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;

SELECT CAST((1, 2) AS Nullable(Tuple(UInt32, UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;
SELECT CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;

SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 0;
SELECT tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, 42) SETTINGS transform_null_in = 0;

SET enable_analyzer = 0;

SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, 42) SETTINGS transform_null_in = 1;

SELECT tuple(1, 2)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;

SELECT CAST((1, 2) AS Tuple(UInt32, UInt32)) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;
SELECT CAST((3, 4) AS Tuple(UInt32, UInt32)) IN (NULL, (1, 2)) SETTINGS transform_null_in = 1;

SELECT CAST((1, 2) AS Nullable(Tuple(UInt32, UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;
SELECT CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1;

SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 0;
SELECT tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, 42) SETTINGS transform_null_in = 0;
