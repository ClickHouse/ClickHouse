CREATE TABLE 02987_logical_optimizer_table (key Int, value Int) ENGINE=Memory();
CREATE VIEW v1 AS SELECT * FROM 02987_logical_optimizer_table;
CREATE TABLE 02987_logical_optimizer_merge AS v1 ENGINE=Merge(currentDatabase(), 'v1');

SELECT _table, key FROM 02987_logical_optimizer_merge WHERE (_table = toFixedString(toFixedString(toFixedString('v1', toNullable(2)), 2), 2)) OR ((value = toLowCardinality(toNullable(10))) AND (_table = toFixedString(toNullable('v3'), 2))) OR ((value = 20) AND (_table = toFixedString(toFixedString(toFixedString('v1', 2), 2), 2)) AND (_table = toFixedString(toLowCardinality(toFixedString('v3', 2)), 2))) SETTINGS enable_analyzer = true, join_use_nulls = true, convert_query_to_cnf = true;
