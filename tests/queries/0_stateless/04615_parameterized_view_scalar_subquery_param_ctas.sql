-- A scalar subquery as a parameterized view parameter must select the correct view
-- instance in CREATE TABLE ... AS SELECT. The view storage synthesized during
-- only-analyze header inference (`resolveTableFunction` -> `buildParameterizedViewStorage`)
-- uses a placeholder parameter value, but it is used only to infer the header;
-- the population runs a fresh non-only-analyze analysis with the real scalar value.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS date_table_pv_ctas;
DROP VIEW IF EXISTS date_pv_ctas;
DROP TABLE IF EXISTS dst_pv_ctas;

CREATE TABLE date_table_pv_ctas (id Int32, dt Date) ENGINE = Memory;
INSERT INTO date_table_pv_ctas VALUES (1, '2000-01-01'), (2, '1999-12-31'), (3, '1974-04-07');

CREATE VIEW date_pv_ctas AS SELECT * FROM date_table_pv_ctas WHERE dt = {dtparam:Date};

SELECT '-- plain SELECT';
SELECT id FROM date_pv_ctas(dtparam = (SELECT dt FROM date_table_pv_ctas WHERE id = 2));

SELECT '-- CREATE TABLE ... AS SELECT';
CREATE TABLE dst_pv_ctas ENGINE = Memory AS
SELECT id FROM date_pv_ctas(dtparam = (SELECT dt FROM date_table_pv_ctas WHERE id = 2));
SELECT * FROM dst_pv_ctas;

DROP TABLE dst_pv_ctas;
DROP VIEW date_pv_ctas;
DROP TABLE date_table_pv_ctas;
