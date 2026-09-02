DROP TABLE IF EXISTS create_as_select_01021;
CREATE TABLE create_as_select_01021 engine=Memory AS (SELECT (1, 1));
SELECT * FROM create_as_select_01021;
DROP TABLE create_as_select_01021;
