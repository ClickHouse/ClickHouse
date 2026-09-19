-- A parameterized view keeps the query parameters of its `SETTINGS` clause unresolved until the
-- view is called, so deciding the `WITH` alias scope of the stored body must not evaluate them.
-- The parser puts `name = {parameter:Type}` into `ASTSetQuery::query_parameters` rather than into
-- `ASTSetQuery::changes`, which is the list the alias scope reads.

SET param_ver = '24.8';
SET param_flag = 1;

DROP VIEW IF EXISTS pv_compatibility;
DROP VIEW IF EXISTS pv_global_with;

CREATE VIEW pv_compatibility AS SELECT 1 AS x SETTINGS compatibility = {ver:String};
CREATE VIEW pv_global_with AS
    WITH src AS (SELECT 2 AS id)
    SELECT (SELECT max(id) FROM src) AS x
    SETTINGS enable_global_with_statement = {flag:UInt8};

-- The placeholder is stored, not the value it had at creation time.
SELECT name, position(create_table_query, '{ver:String}') > 0 OR position(create_table_query, '{flag:UInt8}') > 0
FROM system.tables
WHERE database = currentDatabase() AND name IN ('pv_compatibility', 'pv_global_with')
ORDER BY name;

-- And the view still answers when the value is supplied at call time.
SELECT * FROM pv_compatibility(ver = '24.8');
SELECT * FROM pv_global_with(flag = 1);

DROP VIEW pv_compatibility;
DROP VIEW pv_global_with;
