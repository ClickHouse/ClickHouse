SET enable_analyzer=1;

DROP TABLE IF EXISTS sub_query1;
DROP TABLE IF EXISTS sub_query2;
DROP TABLE IF EXISTS sub_query3;

CREATE TABLE sub_query1 (id Int64) ENGINE=Memory();
CREATE TABLE sub_query2 (id Int64) ENGINE=Memory();
CREATE TABLE sub_query3 (id Int64) ENGINE=Memory();

EXPLAIN header = 1, pretty = 1
SELECT
    *,
    _table
FROM merge(currentDatabase(), '^s');
                                                                                                                                              
DROP TABLE sub_query1;
DROP TABLE sub_query2;
DROP TABLE sub_query3;