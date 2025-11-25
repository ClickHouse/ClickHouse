-- Repro for dotted alias in views/subqueries misinterpreted as subcolumns when checking nullability
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP VIEW IF EXISTS reproduction_view;
DROP TABLE IF EXISTS table_a;

CREATE TABLE table_a (
    id UInt64,
    name Nullable(String)
) ENGINE = Memory;

INSERT INTO table_a VALUES (1, 'exists'), (2, NULL);

CREATE VIEW reproduction_view AS
SELECT id, name AS `Foo.Bar`
FROM table_a;

SELECT count() FROM reproduction_view WHERE `Foo.Bar` IS NOT NULL;
SELECT count() FROM reproduction_view WHERE `Foo.Bar` IS NULL;
SELECT count(`Foo.Bar`) FROM reproduction_view;

SELECT sum(isNotNull(`Foo.Bar`)) FROM reproduction_view;
SELECT sum(isNull(`Foo.Bar`)) FROM reproduction_view;

SELECT count() FROM (SELECT id, name AS `Foo.Bar` FROM table_a) WHERE `Foo.Bar` IS NOT NULL;
SELECT count() FROM (SELECT id, name AS `Foo.Bar` FROM table_a) WHERE `Foo.Bar` IS NULL;
SELECT sum(isNotNull(`Foo.Bar`)) FROM (SELECT id, name AS `Foo.Bar` FROM table_a);

SELECT isNotNull(`Foo.Bar`) FROM reproduction_view ORDER BY id;

DROP VIEW reproduction_view;
DROP TABLE table_a;
