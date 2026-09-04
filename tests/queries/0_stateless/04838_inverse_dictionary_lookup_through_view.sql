-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

SET explain_query_plan_default = 'legacy';

-- { echo }

SET enable_analyzer = 1;
SET optimize_or_like_chain = 0;

DROP VIEW IF EXISTS view_lookup_nested_v;
DROP VIEW IF EXISTS view_lookup_v;
DROP VIEW IF EXISTS view_lookup_grouped_v;
DROP VIEW IF EXISTS view_lookup_no_key_v;
DROP VIEW IF EXISTS view_lookup_cast_v;
DROP VIEW IF EXISTS view_lookup_cast_key_v;
DROP TABLE IF EXISTS view_lookup_ref;
DROP TABLE IF EXISTS view_lookup_data;
DROP DICTIONARY IF EXISTS view_lookup_dict;

CREATE TABLE view_lookup_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO view_lookup_ref VALUES (4242, 'match');

CREATE DICTIONARY view_lookup_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'view_lookup_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE view_lookup_data (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES view_lookup_data;
INSERT INTO view_lookup_data SELECT number FROM numbers(10000);

-- A plain pass-through view exposing both the dictGet(...)-defined column and its key column.
CREATE VIEW view_lookup_v AS
SELECT id, dictGetString('view_lookup_dict', 'name', id) AS name FROM view_lookup_data;

-- With optimization: predicate through the view is rewritten the same way as a direct dictGet(...) call.
SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_v WHERE name = 'match';

-- Without optimization: dictGet(...) stays behind the view column, so all granules are scanned.
SET optimize_inverse_dictionary_lookup = 0;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_v WHERE name = 'match';

-- Results are the same with the optimization on or off.
SET optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM view_lookup_v WHERE name = 'match';

SET optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM view_lookup_v WHERE name = 'match';

-- A view with GROUP BY is not a safe 1:1 pass-through: the rewrite must not fire.
CREATE VIEW view_lookup_grouped_v AS
SELECT id, dictGetString('view_lookup_dict', 'name', id) AS name, count() AS cnt
FROM view_lookup_data
GROUP BY id, name;

SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_grouped_v WHERE name = 'match';

-- A view that does not expose the dictGet(...) key column has nothing to rewrite the predicate to.
CREATE VIEW view_lookup_no_key_v AS
SELECT dictGetString('view_lookup_dict', 'name', id) AS name FROM view_lookup_data;

SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_no_key_v WHERE name = 'match';

-- A view whose declared column type differs from the inner dictGet(...) result must not be rewritten:
-- a cast sits between the two that this pass does not account for, so the match is rejected by the
-- type check in tryResolveColumnDefinition.
CREATE VIEW view_lookup_cast_v (id UInt64, name FixedString(10)) AS
SELECT id, dictGetString('view_lookup_dict', 'name', id) AS name FROM view_lookup_data;

SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_cast_v WHERE name = 'match';

-- Results are still the same with the optimization on or off, even though it can't rewrite here.
SET optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM view_lookup_cast_v WHERE name = 'match';

SET optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM view_lookup_cast_v WHERE name = 'match';

-- A view whose declared KEY column type differs from the inner table's type still rewrites safely:
-- the mismatch surfaces as an explicit CAST in the resulting condition, and results still match.
CREATE VIEW view_lookup_cast_key_v (id Nullable(UInt64), name String) AS
SELECT id, dictGetString('view_lookup_dict', 'name', id) AS name FROM view_lookup_data;

SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_cast_key_v WHERE name = 'match';

SET optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM view_lookup_cast_key_v WHERE name = 'match';

SET optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM view_lookup_cast_key_v WHERE name = 'match';

-- A view over another view (recursion through the TableNode/view path twice) still rewrites.
CREATE VIEW view_lookup_nested_v AS SELECT * FROM view_lookup_v;

SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM view_lookup_nested_v WHERE name = 'match';

-- A plain subquery (QueryNode source, no view/StorageView involved) also rewrites. `id` must stay
-- referenced in the outer query, otherwise unused-column elimination removes it from the
-- subquery's projection before this pass runs, and the key can no longer be re-expressed.
SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT id FROM (SELECT id, dictGetString('view_lookup_dict', 'name', id) AS name FROM view_lookup_data) AS view_lookup_sub
WHERE name = 'match';

DROP VIEW view_lookup_v;
DROP VIEW view_lookup_grouped_v;
DROP VIEW view_lookup_no_key_v;
DROP VIEW view_lookup_cast_v;
DROP VIEW view_lookup_cast_key_v;
DROP VIEW view_lookup_nested_v;
DROP DICTIONARY view_lookup_dict;
DROP TABLE view_lookup_data;
DROP TABLE view_lookup_ref;
