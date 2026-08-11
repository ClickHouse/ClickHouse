-- Reading back the definition of a view with a recursive CTE must not depend on the analyzer setting:
-- only executing such a query requires the analyzer.

DROP TABLE IF EXISTS recursive_cte_view;
DROP TABLE IF EXISTS recursive_cte_mv;
DROP TABLE IF EXISTS recursive_cte_source;

SET enable_analyzer = 1;

CREATE TABLE recursive_cte_source (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO recursive_cte_source VALUES (1);

CREATE VIEW recursive_cte_view AS
WITH RECURSIVE chain AS
(
    SELECT n FROM recursive_cte_source
    UNION ALL
    SELECT n + 1 FROM chain WHERE n < 3
)
SELECT * FROM chain;

CREATE MATERIALIZED VIEW recursive_cte_mv ENGINE = MergeTree ORDER BY n AS
WITH RECURSIVE chain AS
(
    SELECT n FROM recursive_cte_source
    UNION ALL
    SELECT n + 1 FROM chain WHERE n < 3
)
SELECT * FROM chain;

SELECT sum(n) FROM recursive_cte_view;
SELECT arrayStringConcat(dependencies_table, ',') FROM system.tables WHERE database = currentDatabase() AND name = 'recursive_cte_source';

DETACH TABLE recursive_cte_view;
DETACH TABLE recursive_cte_mv;

SET enable_analyzer = 0;

ATTACH TABLE recursive_cte_view;
ATTACH TABLE recursive_cte_mv;

-- The dependency of the materialized view on its source table is computed from the stored definition
SELECT arrayStringConcat(dependencies_table, ',') FROM system.tables WHERE database = currentDatabase() AND name = 'recursive_cte_source';
SELECT position(create_table_query, 'WITH RECURSIVE') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'recursive_cte_mv';

SELECT sum(n) FROM recursive_cte_view SETTINGS enable_analyzer = 1;

WITH RECURSIVE chain AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM chain WHERE n < 3)
SELECT * FROM chain; -- { serverError UNSUPPORTED_METHOD }

SELECT * FROM recursive_cte_view; -- { serverError UNSUPPORTED_METHOD }

DROP TABLE recursive_cte_view;
DROP TABLE recursive_cte_mv;
DROP TABLE recursive_cte_source;
