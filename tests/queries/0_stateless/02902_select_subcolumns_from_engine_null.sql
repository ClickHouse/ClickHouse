CREATE TABLE null_02902 (t Tuple(num Int64, str String)) ENGINE = Null;
SELECT t FROM null_02902;
SELECT tupleElement(t, 'num') FROM null_02902;
SELECT t.num, t.str FROM null_02902;

DROP TABLE null_02902;
