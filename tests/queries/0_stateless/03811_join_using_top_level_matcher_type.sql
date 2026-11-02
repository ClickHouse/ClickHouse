DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (a Int32, b String, a1 Int32 ALIAS a+1) ENGINE = Memory;
CREATE TABLE t1 (a Int16, b String, a1 Int16 ALIAS a+1) ENGINE = Memory;
INSERT INTO t0 VALUES (2, 'test');
INSERT INTO t1 VALUES (2, 'test');

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT tuple(*), 2 a FROM t0 JOIN t1 USING (a);
SELECT tuple(t1.*), 2 a FROM t0 JOIN t1 USING (a);
SELECT tuple(t0.*), 2 a FROM t0 JOIN t1 USING (a);
SELECT tuple(t1.a, t0.a, a), 2 a FROM t0 JOIN t1 USING (a);

SELECT tuple(*) FROM t0 JOIN t1 USING (a1);
SELECT tuple(*), 2 a1 FROM t0 JOIN t1 USING (a1) SETTINGS enable_join_runtime_filters=0;
SELECT tuple(*), a1::UInt8 a1 FROM t0 JOIN t1 USING (a1) SETTINGS enable_join_runtime_filters=0;
