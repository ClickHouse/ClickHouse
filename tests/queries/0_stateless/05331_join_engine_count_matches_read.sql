-- count() and system.tables.total_rows of a Join table equal the number of rows a read returns.

CREATE TABLE all_left (key String, a UInt64) ENGINE = Join(ALL, LEFT, key);
CREATE TABLE all_inner (key String, a UInt64) ENGINE = Join(ALL, INNER, key);
CREATE TABLE all_right (key String, a UInt64) ENGINE = Join(ALL, RIGHT, key);
CREATE TABLE all_full (key String, a UInt64) ENGINE = Join(ALL, FULL, key);
CREATE TABLE any_left (key String, a UInt64) ENGINE = Join(ANY, LEFT, key);
CREATE TABLE any_inner (key String, a UInt64) ENGINE = Join(ANY, INNER, key);
CREATE TABLE any_right (key String, a UInt64) ENGINE = Join(ANY, RIGHT, key);
CREATE TABLE semi_left (key String, a UInt64) ENGINE = Join(SEMI, LEFT, key);
CREATE TABLE semi_right (key String, a UInt64) ENGINE = Join(SEMI, RIGHT, key);
CREATE TABLE anti_left (key String, a UInt64) ENGINE = Join(ANTI, LEFT, key);
CREATE TABLE anti_right (key String, a UInt64) ENGINE = Join(ANTI, RIGHT, key);
CREATE TABLE all_inner_two_keys (k1 UInt32, k2 UInt32, a UInt64) ENGINE = Join(ALL, INNER, k1, k2);
CREATE TABLE all_right_nullable (key Nullable(String), a UInt64) ENGINE = Join(ALL, RIGHT, key);

INSERT INTO all_left VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO all_inner VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO all_right VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO all_full VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO any_left VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO any_inner VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO any_right VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO semi_left VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO semi_right VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO anti_left VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO anti_right VALUES ('k1', 10), ('k1', 20), ('k2', 30);
INSERT INTO all_inner_two_keys VALUES (1, 1, 10), (1, 1, 20), (1, 2, 30);
INSERT INTO all_right_nullable VALUES ('k1', 10), ('k1', 20), (NULL, 30);

SELECT 'all_left', count() FROM all_left SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_left', count() FROM all_left SETTINGS optimize_trivial_count_query = 0;
SELECT 'all_inner', count() FROM all_inner SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_inner', count() FROM all_inner SETTINGS optimize_trivial_count_query = 0;
SELECT 'all_right', count() FROM all_right SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_right', count() FROM all_right SETTINGS optimize_trivial_count_query = 0;
SELECT 'all_full', count() FROM all_full SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_full', count() FROM all_full SETTINGS optimize_trivial_count_query = 0;
SELECT 'any_left', count() FROM any_left SETTINGS optimize_trivial_count_query = 1;
SELECT 'any_left', count() FROM any_left SETTINGS optimize_trivial_count_query = 0;
SELECT 'any_inner', count() FROM any_inner SETTINGS optimize_trivial_count_query = 1;
SELECT 'any_inner', count() FROM any_inner SETTINGS optimize_trivial_count_query = 0;
SELECT 'any_right', count() FROM any_right SETTINGS optimize_trivial_count_query = 1;
SELECT 'any_right', count() FROM any_right SETTINGS optimize_trivial_count_query = 0;
SELECT 'semi_left', count() FROM semi_left SETTINGS optimize_trivial_count_query = 1;
SELECT 'semi_left', count() FROM semi_left SETTINGS optimize_trivial_count_query = 0;
SELECT 'semi_right', count() FROM semi_right SETTINGS optimize_trivial_count_query = 1;
SELECT 'semi_right', count() FROM semi_right SETTINGS optimize_trivial_count_query = 0;
SELECT 'anti_left', count() FROM anti_left SETTINGS optimize_trivial_count_query = 1;
SELECT 'anti_left', count() FROM anti_left SETTINGS optimize_trivial_count_query = 0;
SELECT 'anti_right', count() FROM anti_right SETTINGS optimize_trivial_count_query = 1;
SELECT 'anti_right', count() FROM anti_right SETTINGS optimize_trivial_count_query = 0;
SELECT 'all_inner_two_keys', count() FROM all_inner_two_keys SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_inner_two_keys', count() FROM all_inner_two_keys SETTINGS optimize_trivial_count_query = 0;
SELECT 'all_right_nullable', count() FROM all_right_nullable SETTINGS optimize_trivial_count_query = 1;
SELECT 'all_right_nullable', count() FROM all_right_nullable SETTINGS optimize_trivial_count_query = 0;

SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT 'remote', count() FROM remote('127.0.0.2', currentDatabase(), all_left) SETTINGS optimize_trivial_count_query = 1;
