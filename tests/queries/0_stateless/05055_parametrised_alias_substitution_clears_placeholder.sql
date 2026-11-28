-- Substituting a parametrised alias must also drop the placeholder, otherwise every later consumer of the
-- node (formatting, tree hash, AST JSON) keeps seeing `{p:Identifier}` instead of the resolved alias.

SET param_p = 'resolved_alias';

DROP VIEW IF EXISTS 05055_view;
CREATE VIEW 05055_view AS SELECT 1 AS {p:Identifier};

SELECT replaceRegexpOne(create_table_query, '^.*\\) AS ', '') FROM system.tables WHERE database = currentDatabase() AND name = '05055_view';
SELECT resolved_alias FROM 05055_view;

DROP VIEW 05055_view;
