-- `cuckoo_filter` and `binary_fuse_filter` must decline, not throw, for a set with NULL
-- over a non-nullable key, and fold `equals` with an unknown enum name to constant false
-- when `validate_enum_literals_in_operators = 0`, like `bloom_filter` does.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

DROP TABLE IF EXISTS t_cuckoo;
DROP TABLE IF EXISTS t_fuse;

CREATE TABLE t_cuckoo
(
    k UInt64,
    e Enum8('a' = 1, 'b' = 2),
    INDEX idx_k k TYPE cuckoo_filter GRANULARITY 1,
    INDEX idx_e e TYPE cuckoo_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 16;

CREATE TABLE t_fuse
(
    k UInt64,
    e Enum8('a' = 1, 'b' = 2),
    INDEX idx_k k TYPE binary_fuse_filter GRANULARITY 1,
    INDEX idx_e e TYPE binary_fuse_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 16;

INSERT INTO t_cuckoo SELECT number, if(number % 2, 'a', 'b') FROM numbers(100);
INSERT INTO t_fuse SELECT number, if(number % 2, 'a', 'b') FROM numbers(100);

SELECT count() FROM t_cuckoo WHERE k IN (NULL, 1, 50);
SELECT count() FROM t_fuse WHERE k IN (NULL, 1, 50);
SELECT count() FROM t_cuckoo WHERE k IN (SELECT toNullable(number) FROM numbers(3));
SELECT count() FROM t_fuse WHERE k IN (SELECT toNullable(number) FROM numbers(3));

SET validate_enum_literals_in_operators = 0;
SELECT count() FROM t_cuckoo WHERE e = 'missing';
SELECT count() FROM t_fuse WHERE e = 'missing';
SELECT count() FROM t_cuckoo WHERE e = 'a';
SELECT count() FROM t_fuse WHERE e = 'a';

DROP TABLE t_cuckoo;
DROP TABLE t_fuse;
