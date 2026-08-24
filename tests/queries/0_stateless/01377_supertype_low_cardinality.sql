SELECT 'hello' UNION ALL SELECT toLowCardinality('hello');
SELECT toTypeName(x) FROM (SELECT 'hello' AS x UNION ALL SELECT toLowCardinality('hello'));

SELECT '---';

create temporary table t1(a String);
create temporary table t2(a LowCardinality(String));
select a from t1 union all select a from t2;

SELECT '---';

CREATE TEMPORARY TABLE a (x String);
CREATE TEMPORARY TABLE b (x LowCardinality(String));
CREATE TEMPORARY TABLE c (x Nullable(String));
CREATE TEMPORARY TABLE d (x LowCardinality(Nullable(String)));

INSERT INTO a VALUES ('hello');
INSERT INTO b VALUES ('hello');
INSERT INTO c VALUES ('hello');
INSERT INTO d VALUES ('hello');

SELECT x FROM a;
SELECT x FROM b;
SELECT x FROM c;
SELECT x FROM d;

SELECT '---';

SELECT x FROM a UNION ALL SELECT x FROM b;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM b UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM b;
SELECT '-';
SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM a;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM c;
SELECT '-';
SELECT x FROM d UNION ALL SELECT x FROM b;

SELECT '---';

SELECT x FROM b UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM d;
SELECT '-';
SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c;

SELECT '---';

SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c UNION ALL SELECT x FROM d;

SELECT '---';

SELECT [CAST('abc' AS LowCardinality(String)), CAST('def' AS Nullable(String))];
SELECT [CAST('abc' AS LowCardinality(String)), CAST('def' AS FixedString(3))];
SELECT [CAST('abc' AS LowCardinality(String)), CAST('def' AS LowCardinality(FixedString(3)))];
