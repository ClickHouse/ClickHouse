DROP TABLE IF EXISTS nest;
CREATE TABLE nest (nested_field Nested(e1 Int32)) ENGINE = MergeTree() ORDER BY nested_field.e1;
INSERT INTO nest (nested_field.e1) VALUES ([1, 2, 3]);
ALTER TABLE nest ADD COLUMN nested_field.e2 Array(Tuple(some_value Int32));
OPTIMIZE TABLE nest FINAL;
SELECT * FROM nest;

DROP TABLE IF EXISTS nest_2;
CREATE TABLE nest_2 (nested_field Nested(e1 Int32)) ENGINE = MergeTree() ORDER BY nested_field.e1;
INSERT INTO nest_2 (nested_field.e1) VALUES ([1, 2, 3]);
ALTER TABLE nest_2 ADD COLUMN nested_field.e2 Array(Tuple(some_value Tuple(another_value Int32)));
OPTIMIZE TABLE nest_2 FINAL;
SELECT * FROM nest_2;
