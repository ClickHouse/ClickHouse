-- Validation and edge cases for the GeoJSON output format.

-- The column named `id` becomes the Feature id, which GeoJSON requires to be a string or number, so a
-- column of any other type (such as an array) is rejected.
SELECT [1, 2] AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }

-- A null id is omitted from the Feature, including when it arrives through a `LowCardinality(Nullable)`
-- column rather than a plain `Nullable` column.
SELECT CAST(NULL AS LowCardinality(Nullable(String))) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
