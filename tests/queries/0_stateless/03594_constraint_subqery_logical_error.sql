CREATE TABLE check_constraint (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE check_constraint (c0) VALUES (1);
ALTER TABLE check_constraint ADD CONSTRAINT c0 CHECK (SELECT 1);
INSERT INTO TABLE check_constraint (c0) VALUES (1); -- { serverError UNKNOWN_IDENTIFIER }
SELECT 1 FROM check_constraint WHERE 1 = 1 SETTINGS optimize_substitute_columns = 1, convert_query_to_cnf = 1;

CREATE TABLE assume_constraint (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE assume_constraint ADD CONSTRAINT c0 ASSUME (SELECT 1);
INSERT INTO TABLE assume_constraint (c0) VALUES (1);
SELECT 1 FROM assume_constraint WHERE 1 = 1 SETTINGS optimize_substitute_columns = 1, convert_query_to_cnf = 1;
