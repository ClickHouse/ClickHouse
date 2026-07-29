-- Tags: no-fasttest
--       no-fasttest: needs the Parquet format.

-- `CREATE TABLE ... AS file()` persists the columns inferred at DDL time, but file-like table
-- functions rebuild the nested storage from the *current* external schema and ignore those cached
-- columns. `StorageTableFunctionProxy::read` hands PREWHERE straight to the nested storage and only
-- converts types in a step *after* the read, so a column whose cached type drifted from the actual
-- one had PREWHERE built against the cached type and re-derived against the actual one - the same
-- `Unexpected return type from notEquals. Expected Nullable(UInt8). Got UInt8` as the nested-`Merge`
-- case. `StorageProxy` forwarding does not cover this: `supportedPrewhereColumns()` is a set of
-- names, and the nested storage does support the column - only its type differs.

-- Pin the inference, which CI randomizes: the drift here must come from the file, not the setting.
SET schema_inference_make_columns_nullable = 'auto';

DROP TABLE IF EXISTS tf_drift;

INSERT INTO FUNCTION file('04653_tf_proxy_drift.parquet', Parquet, 'x Nullable(UInt64), y UInt64')
SELECT number, number + 1 FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;

CREATE TABLE tf_drift AS file('04653_tf_proxy_drift.parquet');

SELECT '-- columns cached in metadata at DDL time --';
SELECT name, type FROM system.columns
WHERE table = 'tf_drift' AND database = currentDatabase() ORDER BY name;

-- The external schema drifts: `x` is no longer Nullable. The cached metadata still says it is.
INSERT INTO FUNCTION file('04653_tf_proxy_drift.parquet', Parquet, 'x UInt64, y UInt64')
SELECT number, number + 1 FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;

DETACH TABLE tf_drift;
ATTACH TABLE tf_drift;

SELECT '-- a plain read still works: the conversion step after read fixes the type up --';
SELECT x, y FROM tf_drift ORDER BY x LIMIT 3;

SELECT '-- PREWHERE on the drifted column must be rejected, not abort --';
SELECT x, y FROM tf_drift PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a column whose type did not drift still supports PREWHERE --';
SELECT count() FROM tf_drift PREWHERE y != 0;
SELECT x, y FROM tf_drift PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM tf_drift WHERE x != 0;
SELECT count() FROM tf_drift WHERE y != 0;

DROP TABLE tf_drift;
