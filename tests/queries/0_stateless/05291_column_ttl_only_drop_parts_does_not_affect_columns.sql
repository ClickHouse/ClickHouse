-- `ttl_only_drop_parts` applies only to the TTLs that delete rows: a merge still clears the values of a
-- column TTL that have expired while other values of the column have not. Only `ttl_only_drop_columns`
-- makes the column keep its values until all of them have expired.

DROP TABLE IF EXISTS t_ttl_drop_parts;
DROP TABLE IF EXISTS t_ttl_drop_columns;

CREATE TABLE t_ttl_drop_parts
(
    d Date,
    key UInt64,
    value String TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0;

CREATE TABLE t_ttl_drop_columns AS t_ttl_drop_parts;
ALTER TABLE t_ttl_drop_columns MODIFY SETTING ttl_only_drop_parts = 0, ttl_only_drop_columns = 1;

INSERT INTO t_ttl_drop_parts VALUES ('2020-01-01', 1, 'expired'), ('2100-01-01', 2, 'alive');
INSERT INTO t_ttl_drop_columns VALUES ('2020-01-01', 1, 'expired'), ('2100-01-01', 2, 'alive');

OPTIMIZE TABLE t_ttl_drop_parts FINAL;
OPTIMIZE TABLE t_ttl_drop_columns FINAL;

SELECT 'ttl_only_drop_parts';
SELECT key, value FROM t_ttl_drop_parts ORDER BY key;
SELECT 'ttl_only_drop_columns';
SELECT key, value FROM t_ttl_drop_columns ORDER BY key;

DROP TABLE t_ttl_drop_parts;
DROP TABLE t_ttl_drop_columns;
