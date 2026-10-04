-- Query parameters scoped to a subquery must survive the analyzer's QueryNode
-- round trip used by the `view` table function.
SELECT * FROM view(SELECT {x:UInt64} AS x SETTINGS param_x = '1');
