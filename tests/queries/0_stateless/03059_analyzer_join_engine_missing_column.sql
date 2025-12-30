-- https://github.com/ClickHouse/ClickHouse/issues/17710
SET enable_analyzer=1;
CREATE TABLE id_val(id UInt32, val UInt32) ENGINE = Memory;
CREATE TABLE id_val_join0(id UInt32, val UInt8) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 0;
CREATE TABLE id_val_join1(id UInt32, val UInt8) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1;

SELECT * FROM id_val ANY LEFT JOIN id_val_join0 USING (id) SETTINGS join_use_nulls = 0;

SELECT * FROM id_val ANY LEFT JOIN id_val_join1 USING (id) SETTINGS join_use_nulls = 1;
