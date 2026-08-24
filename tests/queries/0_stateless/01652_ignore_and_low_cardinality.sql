set allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lc_null_int8_defnull (val LowCardinality(Nullable(Int8)) DEFAULT NULL) ENGINE = MergeTree order by tuple();
SELECT ignore(10, ignore(*), ignore(ignore(-2, 1025, *)), NULL, *), * FROM lc_null_int8_defnull AS values;


SELECT ignore(toLowCardinality(1), toLowCardinality(2), 3);

DROP TABLE lc_null_int8_defnull;
