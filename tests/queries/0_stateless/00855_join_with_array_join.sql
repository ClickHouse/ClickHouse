SET joined_subquery_requires_alias = 0;
SET enable_analyzer = 1;

SELECT ax, c FROM (SELECT [1,2] ax, 0 c) ARRAY JOIN ax JOIN (SELECT 0 c) USING (c);
SELECT ax, c FROM (SELECT [3,4] ax, 0 c) JOIN (SELECT 0 c) USING (c) ARRAY JOIN ax;
SELECT ax, c FROM (SELECT [5,6] ax, 0 c) s1 JOIN system.one s2 ON s1.c = s2.dummy ARRAY JOIN ax;
SELECT ax, c, d FROM (SELECT [7,8] ax, 1 c, 0 d) s1 JOIN system.one s2 ON s1.c = s2.dummy OR s1.d = s2.dummy ARRAY JOIN ax;


SELECT ax, c FROM (SELECT [101,102] ax, 0 c) s1
JOIN system.one s2 ON s1.c = s2.dummy
JOIN system.one s3 ON s1.c = s3.dummy
ARRAY JOIN ax;

SELECT '-';

SET joined_subquery_requires_alias = 1;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;

CREATE TABLE f (`d_ids` Array(Int64) ) ENGINE = TinyLog;
INSERT INTO f VALUES ([1, 2]);

CREATE TABLE d (`id` Int64, `name` String ) ENGINE = TinyLog;

INSERT INTO d VALUES (2, 'a2'), (3, 'a3');

SELECT d_ids, id, name FROM f LEFT ARRAY JOIN d_ids LEFT JOIN d ON d.id = d_ids ORDER BY id;
SELECT did, id, name FROM f LEFT ARRAY JOIN d_ids as did LEFT JOIN d ON d.id = did ORDER BY id;

SELECT id, name FROM f LEFT ARRAY JOIN d_ids as id LEFT JOIN d ON d.id = id ORDER BY id;

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y ON x.dummy == y.dummy;

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y ON x.dummy + 1 == y.dummy + 1;

SELECT * FROM ( SELECT [dummy, dummy] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN system.one AS y USING dummy;

SELECT * FROM ( SELECT [toUInt32(dummy), toUInt32(dummy)] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN (select toInt32(dummy) as dummy from system.one ) AS y USING dummy;

SELECT dummy > 0, toTypeName(any(dummy)), any(toTypeName(dummy))
FROM ( SELECT [toUInt32(dummy), toUInt32(dummy)] AS dummy FROM system.one ) AS x ARRAY JOIN dummy
JOIN ( SELECT toInt32(dummy) AS dummy FROM system.one ) AS y USING dummy GROUP BY (dummy > 0);

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;
