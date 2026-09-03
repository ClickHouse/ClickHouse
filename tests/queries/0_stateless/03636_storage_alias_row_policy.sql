DROP ROW POLICY IF EXISTS target_policy ON test_table;
DROP ROW POLICY IF EXISTS alias_policy ON test_alias;
DROP TABLE IF EXISTS test_merge;
DROP TABLE IF EXISTS test_alias;
DROP TABLE IF EXISTS test_table;

SET allow_experimental_alias_table_engine = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;

CREATE TABLE test_table
(
    id UInt32,
    tenant_id UInt32,
    active UInt8,
    text String,
    INDEX text_idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS materialize_skip_indexes_on_insert = 1;
INSERT INTO test_table VALUES (1, 1, 1, 'alpha'), (2, 1, 0, 'alpha'), (3, 2, 1, 'alpha'), (4, 2, 0, 'alpha');

CREATE TABLE test_alias ENGINE = Alias('test_table');
CREATE TABLE test_merge (id UInt32, tenant_id UInt32, active UInt8)
    ENGINE = Merge(currentDatabase(), '^test_alias$');

CREATE ROW POLICY target_policy ON test_table FOR SELECT USING tenant_id = 1 TO CURRENT_USER;

-- A policy on the target table is also applied when reading through `Alias`.
SELECT 'Target policy with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Target policy with the analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

-- A target policy is also applied when `Merge` reads through `Alias`.
SELECT 'Target policy through Merge with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_merge SETTINGS enable_analyzer = 0;

SELECT 'Target policy through Merge with the analyzer';
SELECT arraySort(groupArray(id)) FROM test_merge SETTINGS enable_analyzer = 1;

CREATE ROW POLICY alias_policy ON test_alias FOR SELECT USING active = 1 TO CURRENT_USER;

-- Policies on the `Alias` and its target are combined with a logical AND.
SELECT 'Combined policies with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Combined policies with the analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

-- A non-trivial combined policy disables the trivial count optimization.
SELECT 'Combined policies and trivial count disabled with the old analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 0;

SELECT 'Combined policies and trivial count enabled with the old analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;

SELECT 'Combined policies and trivial count disabled with the analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 1, optimize_trivial_count_query = 0;

SELECT 'Combined policies and trivial count enabled with the analyzer';
SELECT count() FROM test_alias SETTINGS enable_analyzer = 1, optimize_trivial_count_query = 1;

DROP ROW POLICY target_policy ON test_table;

SELECT 'Alias policy with the old analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 0;

SELECT 'Alias policy with the analyzer';
SELECT arraySort(groupArray(id)) FROM test_alias SETTINGS enable_analyzer = 1;

-- The text-index count optimization must not discard a policy defined only on the `Alias`.
SELECT 'Alias policy and text-index count optimization disabled';
SELECT count() FROM test_alias WHERE hasToken(text, 'alpha')
    SETTINGS enable_analyzer = 1, query_plan_optimize_count_from_text_index = 0;

SELECT 'Alias policy and text-index count optimization enabled';
SELECT count() FROM test_alias WHERE hasToken(text, 'alpha') SETTINGS enable_analyzer = 1;

DROP ROW POLICY alias_policy ON test_alias;
DROP TABLE test_merge;
DROP TABLE test_alias;
DROP TABLE test_table;
