-- Nested isNotNull(json.path) = 0 must not use the bare isNotNull JSON fast path.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

DROP TABLE IF EXISTS t_json_cuckoo;
CREATE TABLE t_json_cuckoo
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE cuckoo_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_cuckoo VALUES (1, '{"b": 1}'), (2, '{"b": 2}');
INSERT INTO t_json_cuckoo VALUES (3, '{"a": 1}'), (4, '{"a": 2}');

SELECT count() FROM t_json_cuckoo WHERE isNotNull(json.a) = 0;

SELECT
    (
        SELECT count()
        FROM t_json_cuckoo
        WHERE isNotNull(json.a)
        SETTINGS force_data_skipping_indices = 'idx'
    ) = (
        SELECT count()
        FROM t_json_cuckoo
        WHERE isNotNull(json.a)
    );

DROP TABLE t_json_cuckoo;

DROP TABLE IF EXISTS t_json_fuse;
CREATE TABLE t_json_fuse
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE binary_fuse_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_fuse VALUES (1, '{"b": 1}'), (2, '{"b": 2}');
INSERT INTO t_json_fuse VALUES (3, '{"a": 1}'), (4, '{"a": 2}');

SELECT count() FROM t_json_fuse WHERE isNotNull(json.a) = 0;

SELECT
    (
        SELECT count()
        FROM t_json_fuse
        WHERE isNotNull(json.a)
        SETTINGS force_data_skipping_indices = 'idx'
    ) = (
        SELECT count()
        FROM t_json_fuse
        WHERE isNotNull(json.a)
    );

DROP TABLE t_json_fuse;
