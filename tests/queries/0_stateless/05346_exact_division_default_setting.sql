SET sql_compatible_division_for_integers = 1;
SELECT q, toTypeName(q) FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = DEFAULT);
SELECT q, toTypeName(q) FROM (SELECT divide(5, 2) AS q SETTINGS sql_compatible_division_for_integers = DEFAULT);
SELECT 5 / 2, (SELECT 5 / 2 SETTINGS sql_compatible_division_for_integers = DEFAULT), 7 / 2;
SELECT q, toTypeName(q) FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = 1, sql_compatible_division_for_integers = DEFAULT);
SELECT q, toTypeName(q) FROM
(
    SELECT q FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = DEFAULT)
    SETTINGS sql_compatible_division_for_integers = 1
);
SELECT q, toTypeName(q) FROM
(
    SELECT q FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = 1)
    SETTINGS sql_compatible_division_for_integers = DEFAULT
);
SELECT 5 / 2, toTypeName(5 / 2) SETTINGS sql_compatible_division_for_integers = DEFAULT;
CREATE VIEW division_default_view AS
    SELECT q FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = DEFAULT);
SELECT q, toTypeName(q) FROM division_default_view;
DETACH TABLE division_default_view;
ATTACH TABLE division_default_view;
SELECT q, toTypeName(q) FROM division_default_view;
SET sql_compatible_division_for_integers = 0;
SELECT q, toTypeName(q) FROM division_default_view;
DROP VIEW division_default_view;
