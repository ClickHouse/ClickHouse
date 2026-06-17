-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: covers ReplicatedMergeTree-specific table setting behavior.
-- Tag no-shared-merge-tree: covers ReplicatedMergeTree-specific table setting behavior.

-- Test for https://github.com/ClickHouse/ClickHouse/issues/106088
-- JSON (Object) column should be forbidden in minmax skip index by default.
-- The MergeTree setting allow_minmax_index_for_json controls persistent table behavior.
-- The query-level setting with the same name can suppress ALTER validation for compatibility,
-- consistent with allow_suspicious_indices.

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

-- Should fail: RESET SETTING while JSON minmax index is still present must be rejected,
-- because after reset the effective value returns to default (false) which contradicts the
-- presence of the JSON minmax index.
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

ALTER TABLE t_json_minmax_forbidden RESET SETTING allow_minmax_index_for_json; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_forbidden;

CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON
) ENGINE = MergeTree() ORDER BY id;

-- Should fail: query-level setting alone cannot persist a JSON minmax index.
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1
    SETTINGS allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should fail: MODIFY SETTING alone does not suppress validation of the current ALTER.
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should fail: even with query-level setting, the table-level setting must already
-- be effective before a new JSON minmax index is created.
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1
    SETTINGS allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should succeed: table-level setting is already effective before adding the index.
ALTER TABLE t_json_minmax_forbidden MODIFY SETTING allow_minmax_index_for_json = 1;
ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated SYNC;

CREATE TABLE t_json_minmax_forbidden_replicated (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array', 'r1') ORDER BY id;

-- Should fail: query-level setting alone cannot persist a JSON minmax index.
ALTER TABLE t_json_minmax_forbidden_replicated
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1
    SETTINGS allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should fail: MODIFY SETTING alone does not suppress validation of the current ALTER.
ALTER TABLE t_json_minmax_forbidden_replicated
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should fail: even with query-level setting, the table-level setting must already
-- be effective before a new replicated JSON minmax index is written to Keeper.
ALTER TABLE t_json_minmax_forbidden_replicated
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1
    SETTINGS allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

-- Should fail: ReplicatedMergeTree table settings are local, but skip index metadata
-- is replicated through Keeper.
ALTER TABLE t_json_minmax_forbidden_replicated MODIFY SETTING allow_minmax_index_for_json = 1;
ALTER TABLE t_json_minmax_forbidden_replicated
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated SYNC;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r1 SYNC;
DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated_r2 SYNC;

CREATE TABLE t_json_minmax_forbidden_replicated_r1 (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_two_replicas', 'r1') ORDER BY id;

CREATE TABLE t_json_minmax_forbidden_replicated_r2 (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array_two_replicas', 'r2') ORDER BY id;

-- Should fail on the initiating replica before writing unsafe index metadata to Keeper.
ALTER TABLE t_json_minmax_forbidden_replicated_r1 MODIFY SETTING allow_minmax_index_for_json = 1;
ALTER TABLE t_json_minmax_forbidden_replicated_r1
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

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

-- Should succeed: ALTER TABLE MODIFY SETTING on a table that already has a JSON minmax index.
-- DETACH/ATTACH skips validation (attach=true), and the query-level setting suppresses validation
-- while making allow_minmax_index_for_json durable in table metadata.
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

DETACH TABLE t_json_minmax_forbidden;
ATTACH TABLE t_json_minmax_forbidden;

-- Recovery path: persist the table-level setting. The query-level setting suppresses validation
-- of the current ALTER.
ALTER TABLE t_json_minmax_forbidden MODIFY SETTING allow_minmax_index_for_json = 1
SETTINGS allow_minmax_index_for_json = 1;

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
