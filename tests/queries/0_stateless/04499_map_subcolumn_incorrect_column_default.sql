-- Reading a subcolumn of a column that is missing in a part must evaluate the column's DEFAULT
-- expression (reading its source columns from the part) instead of substituting type defaults.
-- https://github.com/ClickHouse/ClickHouse/issues/120326
DROP TABLE IF EXISTS t_map_subcolumn_default;

CREATE TABLE t_map_subcolumn_default
(
    id UInt64,
    `a.key` Array(String),
    `a.value` Array(String)
)
ENGINE = MergeTree
ORDER BY id;


INSERT INTO t_map_subcolumn_default
SELECT number, ['k1', 'k2'], ['v1', 'v2']
FROM numbers(10);

ALTER TABLE t_map_subcolumn_default
ADD COLUMN m Map(String, Nullable(String))
DEFAULT mapFromArrays(`a.key`, `a.value`);

SELECT 'BEFORE MATERIALIZE COLUMN';

SELECT count()
FROM t_map_subcolumn_default
WHERE m['k1'] = 'v1';

ALTER TABLE t_map_subcolumn_default MATERIALIZE COLUMN m SETTINGS mutations_sync = 1;

SELECT 'AFTER MATERIALIZE COLUMN';

SELECT count()
FROM t_map_subcolumn_default
WHERE m['k1'] = 'v1';


DROP TABLE IF EXISTS t_map_subcolumn_default;
