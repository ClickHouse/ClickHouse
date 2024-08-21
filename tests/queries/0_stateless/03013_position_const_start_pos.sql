CREATE TABLE 03013_position_const_start_pos (n Int16) ENGINE = Memory;
INSERT INTO 03013_position_const_start_pos SELECT * FROM generateRandom() LIMIT 1000;
SELECT position('test', 'ca', 2) FROM 03013_position_const_start_pos FORMAT Null;
