-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: mixed local/replicated ALTER is rejected earlier by Replicated database validation.
-- Tag no-shared-merge-tree: covers ReplicatedMergeTree-specific local settings vs replicated metadata behavior.

-- Test for https://github.com/ClickHouse/ClickHouse/issues/106088
-- JSON (Object) column should be forbidden in minmax skip index by default.
-- The MergeTree setting allow_minmax_index_for_json controls this behavior.
-- There is no session/query-level setting; the guard is only controlled via
-- table-level SETTINGS (consistent with allow_suspicious_indices).

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

-- Should succeed: a mixed ALTER is validated against the post-alter effective settings.
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON
) ENGINE = MergeTree() ORDER BY id;

ALTER TABLE t_json_minmax_forbidden
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated SYNC;

-- Should fail: for ReplicatedMergeTree, MODIFY SETTING is local but ADD INDEX is replicated.
-- The setting must be enabled before the replicated metadata alter writes the index to Keeper.
CREATE TABLE t_json_minmax_forbidden_replicated (
    id Int32,
    col1 JSON
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04278_json_minmax_index_mixed_type_array', 'r1') ORDER BY id;

ALTER TABLE t_json_minmax_forbidden_replicated
    ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1,
    MODIFY SETTING allow_minmax_index_for_json = 1; -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_json_minmax_forbidden_replicated MODIFY SETTING allow_minmax_index_for_json = 1;
ALTER TABLE t_json_minmax_forbidden_replicated ADD INDEX col_idx col1 TYPE minmax GRANULARITY 1;

DROP TABLE IF EXISTS t_json_minmax_forbidden_replicated SYNC;

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

-- Should succeed: ALTER TABLE MODIFY SETTING on a legacy table.
-- Simulates the upgrade/recovery path: DETACH/ATTACH skips validation (attach=true),
-- then ALTER TABLE MODIFY SETTING allow_minmax_index_for_json = 1 makes it durable.
-- We create the table with the setting enabled, then detach+attach to simulate a legacy
-- table that loaded without validation.
CREATE TABLE t_json_minmax_forbidden (
    id Int32,
    col1 JSON,
    INDEX col_idx col1 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id
SETTINGS allow_minmax_index_for_json = 1;

DETACH TABLE t_json_minmax_forbidden;
ATTACH TABLE t_json_minmax_forbidden;

-- Recovery path: persist the table-level setting. Should NOT be rejected.
ALTER TABLE t_json_minmax_forbidden MODIFY SETTING allow_minmax_index_for_json = 1;

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
