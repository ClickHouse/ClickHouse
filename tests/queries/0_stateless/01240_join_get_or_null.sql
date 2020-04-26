DROP TABLE IF EXISTS join_test;

CREATE TABLE join_test (id UInt16, num UInt16) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Nullable(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Array(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500); -- { serverError 43 }
DROP TABLE join_test;
