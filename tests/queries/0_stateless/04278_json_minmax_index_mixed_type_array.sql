-- Test for https://github.com/ClickHouse/ClickHouse/issues/106088
-- JSON (Object) column should be forbidden in minmax skip index by default.
-- The MergeTree setting or query-level setting allow_minmax_index_for_json can suppress
-- validation for compatibility, consistent with allow_suspicious_indices.

DROP TABLE IF EXISTS t_json_minmax_forbidden;

-- Should fail: Nullable(JSON) column is not allowed in minmax index (original issue)
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 Nullable(JSON),
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_forbidden;

-- Should fail: non-nullable JSON column also forbidden
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_forbidden;

-- Should fail: Array(JSON) is rejected recursively via forEachChild
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 Array(JSON),
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_forbidden;

-- Should succeed: explicit table-level SETTINGS allow_minmax_index_for_json = 1
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

-- Should succeed: table-level setting keeps later unrelated ALTER valid.
ALTER TABLE t_json_minmax_forbidden ADD COLUMN extra UInt8 DEFAULT 0;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON
) ENGINE = MergeTree() ORDER BY id;

-- Should succeed: query-level setting alone can suppress the current ALTER validation.
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1
    SETTINGS allow_minmax_index_for_json = 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON
) ENGINE = MergeTree() ORDER BY id;

-- Should succeed: table-level setting is already effective before adding the index.
ALTER TABLE t_json_minmax_forbidden MODIFY SETTING allow_minmax_index_for_json = 1;
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_create_r1 SYNC;
DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_create_r2 SYNC;

-- Should succeed: each replica gets the table-level setting from its local CREATE query.
CREATE TABLE t_json_minmax_forbidden_replicated_create_r1 (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_create', 'r1') ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

CREATE TABLE t_json_minmax_forbidden_replicated_create_r2 (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_create', 'r2') ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_create_r1 SYNC;
DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_create_r2 SYNC;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r1 SYNC;
DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r2 SYNC;

CREATE TABLE t_json_minmax_forbidden_replicated_r1 (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_two_replicas', 'r1') ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

CREATE TABLE t_json_minmax_forbidden_replicated_r2 (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_two_replicas', 'r2') ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

-- Should succeed: table-level setting allows the ALTER on the initiating replica,
-- and metadata replay on the follower sees the same table-level setting.
ALTER TABLE t_json_minmax_forbidden_replicated_r1
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1;
SYSTEM SYNC REPLICA t_json_minmax_forbidden_replicated_r2;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r1 SYNC;
DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r2 SYNC;

-- Should succeed: dropping the JSON minmax index and resetting the escape hatch in one ALTER
-- leaves a valid final metadata state.
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

ALTER TABLE t_json_minmax_forbidden
    DROP INDEX col_idx,
    RESET SETTING allow_minmax_index_for_json;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

-- Should succeed: typed subcolumn of JSON in minmax index is fine
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON(a UInt32),
    INDEX col_idx col1.a TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_json_minmax_forbidden VALUES (1, '{"a": 10}');
INSERT INTO t_json_minmax_forbidden VALUES (2, '{"a": 20}');
SELECT id FROM t_json_minmax_forbidden WHERE col1.a < 15 ORDER BY id;

DROP TABLE IF EXISTS t_json_minmax_forbidden;
