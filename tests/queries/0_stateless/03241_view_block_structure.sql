DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS vfoo;

CREATE TABLE foo(to_dttm DateTime) ENGINE = MergeTree() ORDER BY ();

CREATE VIEW vfoo AS
SELECT CAST(foo.to_dttm AS DateTime64(6))       AS feature,
       toDateTime64('2024-03-26 11:35:03.620846', 6) AS to_dttm
FROM foo;

SELECT * FROM vfoo WHERE vfoo.to_dttm=toDateTime64('2024-03-26 11:35:03.620846', 6);

DROP TABLE vfoo;
DROP TABLE foo;

CREATE TABLE foo(to_dttm_blaaaaaaaaaaaaaa DateTime) ENGINE = MergeTree() ORDER BY ();

CREATE VIEW vfoo AS
SELECT CAST(foo.to_dttm_blaaaaaaaaaaaaaa AS DateTime64(6))       AS feature,
       toDateTime64('2024-03-26 11:35:03.620846', 6) AS to_dttm
FROM foo;

SELECT * FROM vfoo WHERE vfoo.to_dttm=toDateTime64('2024-03-26 11:35:03.620846', 6);

DROP TABLE vfoo;
DROP TABLE foo;
