-- Query parameters in CREATE / ALTER ROW POLICY filters are substituted when the policy is stored.
-- The same for MASKING POLICY expressions; masking policies are Cloud-only, so only an unset parameter is checked here.
-- An `Identifier` query parameter in a filter setting is never substituted, so it is rejected.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t (k UInt64) ENGINE = Memory;
INSERT INTO t VALUES (1), (2);
CREATE TABLE keys (k UInt64, a Array(UInt64)) ENGINE = Memory;
INSERT INTO keys VALUES (1, [1]);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.keys (k UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.keys VALUES (2);

CREATE ROW POLICY p ON t USING k IN (SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.keys) TO ALL;
SELECT 'identifier', k FROM t ORDER BY k;
SELECT 'stored without placeholder', position(select_filter, '{') = 0 FROM system.row_policies WHERE database = currentDatabase() AND table = 't' AND short_name = 'p';

SET param_v = '1';
ALTER ROW POLICY p ON t USING k > {v:UInt64};
SELECT 'alter', k FROM t ORDER BY k;

SET param_cond = '1';
ALTER ROW POLICY p ON t USING {cond:UInt8};
SELECT 'whole filter', count() FROM t;

ALTER ROW POLICY p ON t USING k > {unset_param:UInt64}; -- { serverError UNKNOWN_QUERY_PARAMETER }
DROP ROW POLICY p ON t;

CREATE ROW POLICY p2 ON t USING k IN (SELECT k FROM keys LEFT ARRAY JOIN 'abc', {unset_param:Identifier}) TO ALL; -- { serverError UNKNOWN_QUERY_PARAMETER }
SET param_col = 'a';
CREATE ROW POLICY p2 ON t USING k IN (SELECT k FROM keys LEFT ARRAY JOIN 'abc', {col:Identifier}) TO ALL;
SELECT k FROM t; -- { serverError TYPE_MISMATCH }
DROP ROW POLICY p2 ON t;

CREATE MASKING POLICY mp ON t UPDATE k = {unset_param:UInt64} TO ALL; -- { serverError UNKNOWN_QUERY_PARAMETER }
ALTER MASKING POLICY mp ON t UPDATE k = 0 WHERE k > {unset_param:UInt64}; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT k FROM keys LEFT ARRAY JOIN \'abc\', {x:Identifier})'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_result_filter = 'k IN (SELECT k FROM keys LEFT ARRAY JOIN \'abc\', {x:Identifier})'; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT COLUMNS({x:Identifier}) FROM keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT k FROM {x:Identifier}.keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'arrayExists({x:Identifier} -> 1, [k])'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT * EXCEPT ({x:Identifier}) FROM keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT {x:Identifier}.* FROM keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT {x:Identifier}.COLUMNS(k) FROM keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT k FROM t SETTINGS additional_table_filters = {'t': 'k IN (SELECT {x:Identifier}.COLUMNS(\'k\') FROM keys)'}; -- { serverError UNKNOWN_QUERY_PARAMETER }

DROP ROW POLICY IF EXISTS p ON t;
DROP ROW POLICY IF EXISTS p2 ON t;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
