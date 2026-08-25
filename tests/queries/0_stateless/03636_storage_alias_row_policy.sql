DROP ROW POLICY IF EXISTS target_policy ON test_table;
DROP ROW POLICY IF EXISTS alias_policy ON test_alias;
DROP TABLE IF EXISTS test_alias;
DROP TABLE IF EXISTS test_table;

SET allow_experimental_alias_table_engine = 1;

CREATE TABLE test_table (id UInt32, tenant_id UInt32, active UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_table VALUES (1, 1, 1), (2, 1, 0), (3, 2, 1), (4, 2, 0);

CREATE TABLE test_alias ENGINE = Alias('test_table');

CREATE ROW POLICY target_policy ON test_table FOR SELECT USING tenant_id = 1 TO CURRENT_USER;

-- A policy on the target table is also applied when reading through `Alias`.
SELECT 'Target policy with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Target policy with the new analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

CREATE ROW POLICY alias_policy ON test_alias FOR SELECT USING active = 1 TO CURRENT_USER;

-- Policies on the `Alias` and its target are combined with a logical AND.
SELECT 'Combined policies with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Combined policies with the new analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

-- A non-trivial combined policy disables the trivial count optimization.
SELECT 'Combined policies and trivial count disabled with the old analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 0;

SELECT 'Combined policies and trivial count enabled with the old analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;

SELECT 'Combined policies and trivial count disabled with the new analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 1, optimize_trivial_count_query = 0;

SELECT 'Combined policies and trivial count enabled with the new analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 1, optimize_trivial_count_query = 1;

DROP ROW POLICY target_policy ON test_table;

SELECT 'Alias policy with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Alias policy with the new analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

DROP ROW POLICY alias_policy ON test_alias;
DROP TABLE test_alias;
DROP TABLE test_table;
