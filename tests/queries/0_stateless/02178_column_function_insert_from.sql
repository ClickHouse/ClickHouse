DROP TABLE IF EXISTS TESTTABLE;

CREATE TABLE TESTTABLE (
  _id UInt64,  pt String, attr_list Array(String)
) ENGINE = MergeTree() PARTITION BY (pt) ORDER BY tuple();

INSERT INTO TESTTABLE values (0,'0',['1']), (1,'1',['1']);

SET max_threads = 1;

SELECT attr, _id, arrayFilter(x -> (x IN (select '1')), attr_list) z
FROM TESTTABLE ARRAY JOIN z AS attr ORDER BY _id LIMIT 3 BY attr;

DROP TABLE TESTTABLE;
