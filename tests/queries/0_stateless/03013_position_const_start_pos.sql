CREATE TABLE 03013_position_const_start_pos (n Int16) ENGINE = Memory;
INSERT INTO 03013_position_const_start_pos SELECT * FROM generateRandom() LIMIT 1000;
SELECT position(concat(NULLIF(1, 1), materialize(3)), 'ca', 2) FROM 03013_position_const_start_pos FORMAT Null;
