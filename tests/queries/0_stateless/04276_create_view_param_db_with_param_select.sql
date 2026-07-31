-- Regression for STID 3282-3691: CREATE VIEW with a query parameter in the database
-- identifier and another query parameter in the SELECT body must not trigger the
-- `!database_name.empty()` assertion in `DatabaseCatalog::tryGetDatabase`.
--
-- Before the fix, `ASTCreateQuery::isParameterizedView` returned true whenever the
-- SELECT body had any query parameter, which made `executeQueryImpl` skip
-- `ReplaceQueryParameterVisitor` entirely. The `{db:Identifier}` placeholder in the
-- database name was therefore never substituted, leaving `create.getDatabase()`
-- empty and aborting the server in debug builds. The fix runs the visitor on the
-- DDL parts of the parameterized view (database name, table name, columns list,
-- storage, view targets) while keeping the SELECT body's placeholders intact, so
-- they can be substituted later at view-call time.

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.src_04276 (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.src_04276 VALUES (10, 'ten');

-- The database identifier is a query parameter and the SELECT contains another query
-- parameter (the view's parameterizable interface). The exact STID 3282-3691 reproducer
-- from the AST fuzzer hit this `CREATE VIEW` shape (see issue tracker).
CREATE VIEW {CLICKHOUSE_DATABASE:Identifier}.pv_04276 AS
    SELECT k + {n:UInt64} AS x, v
    FROM src_04276;

-- A second variant exercising the same code path with extra DDL parts and FINAL,
-- mirroring the original fuzzer query (test_view_01051_d__fuzz_13).
CREATE VIEW {CLICKHOUSE_DATABASE:Identifier}.pv_04276_fuzz (`key` Nullable(UInt64), `value` LowCardinality(String)) AS
    SELECT k2 + {fuzz_param_0:UInt64} AS key, concat(v2, '_x') AS value
    FROM (SELECT k + 2 AS k2, concat('_y', v) AS v2 FROM src_04276) FINAL;

SELECT 'create view with param db: ok';

DROP VIEW {CLICKHOUSE_DATABASE:Identifier}.pv_04276;
DROP VIEW {CLICKHOUSE_DATABASE:Identifier}.pv_04276_fuzz;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.src_04276;
