-- count() and total_rows of ALL and ANY RIGHT Join tables with a composite String key, which cannot be read with SELECT.

DROP TABLE IF EXISTS join_hashed_all_left;
DROP TABLE IF EXISTS join_hashed_any_right;

CREATE TABLE join_hashed_all_left (k1 String, k2 String, a UInt64) ENGINE = Join(ALL, LEFT, k1, k2);
CREATE TABLE join_hashed_any_right (k1 String, k2 String, a UInt64) ENGINE = Join(ANY, RIGHT, k1, k2);

INSERT INTO join_hashed_all_left VALUES ('k1', 'x', 10), ('k1', 'x', 20), ('k2', 'y', 30);
INSERT INTO join_hashed_any_right VALUES ('k1', 'x', 10), ('k1', 'x', 20), ('k2', 'y', 30);

SELECT 'all_left', count() FROM join_hashed_all_left SETTINGS optimize_trivial_count_query = 1;
SELECT 'any_right', count() FROM join_hashed_any_right SETTINGS optimize_trivial_count_query = 1;
SELECT count() FROM join_hashed_all_left SETTINGS optimize_trivial_count_query = 0; -- { serverError UNSUPPORTED_JOIN_KEYS }
SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() AND name LIKE 'join_hashed_%' ORDER BY name;

DROP TABLE join_hashed_all_left;
DROP TABLE join_hashed_any_right;
