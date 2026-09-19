DROP TABLE IF EXISTS insert_by_name_dst_05229;
DROP TABLE IF EXISTS insert_by_name_src_05229;

CREATE TABLE insert_by_name_dst_05229
(
    a UInt64,
    b String DEFAULT 'missing',
    c UInt64 DEFAULT a + 10,
    d UInt64
)
ENGINE = Memory;

CREATE TABLE insert_by_name_src_05229
(
    b String,
    a UInt64
)
ENGINE = Memory;

CREATE TABLE insert_by_name_input_dst_05229
(
    a String,
    b UInt64
)
ENGINE = Memory;

INSERT INTO insert_by_name_src_05229 VALUES ('hello', 42), ('world', 7);

INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT * FROM insert_by_name_src_05229;
SELECT a, b, c, d FROM insert_by_name_dst_05229 ORDER BY a FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT 'alias' AS b, toInt32(5) AS a;
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
FROM insert_by_name_src_05229
SELECT b, a;
SELECT a, b, c, d FROM insert_by_name_dst_05229 ORDER BY a FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT * EXCEPT(a) FROM insert_by_name_src_05229;
SELECT a, b, c, d FROM insert_by_name_dst_05229 ORDER BY b FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b, a FROM input('a UInt64, b String') FORMAT TSV
100	input
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT a, b FROM input() FORMAT TSV
42	hello
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b FROM input() FORMAT TSV
hello
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b, a FROM input() FORMAT TSV
hello	100
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

INSERT INTO insert_by_name_input_dst_05229 BY NAME
SELECT b FROM input()
WHERE b % 2 = 0
FORMAT TSV
2
SELECT a, b FROM insert_by_name_input_dst_05229 FORMAT TSVRaw;

TRUNCATE TABLE insert_by_name_input_dst_05229;
INSERT INTO insert_by_name_input_dst_05229 (b)
SELECT b FROM input()
WHERE b % 2 = 0
FORMAT TSV
2
SELECT a, b FROM insert_by_name_input_dst_05229 FORMAT TSVRaw;

SET use_structure_from_insertion_table_in_table_functions = 1;
INSERT INTO FUNCTION file(concat(database(), '.data_05229_insert_by_name.bin'), RowBinary)
SELECT 'file' AS b, 100 AS a
SETTINGS engine_file_truncate_on_insert = 1;
TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b, a FROM file(concat(database(), '.data_05229_insert_by_name.bin'), RowBinary);
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;

SET allow_experimental_analyzer = 0;
INSERT INTO FUNCTION file(concat(database(), '.data_05229_insert_by_name_old.bin'), RowBinary)
SELECT 'old' AS b, 101 AS a
SETTINGS engine_file_truncate_on_insert = 1;
TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b, a FROM file(concat(database(), '.data_05229_insert_by_name_old.bin'), RowBinary);
SELECT a, b, c, d FROM insert_by_name_dst_05229 FORMAT TSVRaw;
SET allow_experimental_analyzer = 1;

SET allow_experimental_analyzer = 0;
TRUNCATE TABLE insert_by_name_dst_05229;
INSERT INTO insert_by_name_dst_05229 BY NAME
SELECT b, a FROM insert_by_name_src_05229;
SELECT a, b, c, d FROM insert_by_name_dst_05229 ORDER BY a FORMAT TSVRaw;
SET allow_experimental_analyzer = 1;

SELECT JSONExtractBool(
    parseQueryToJSON('INSERT INTO insert_by_name_dst_05229 BY NAME SELECT 1 AS a'),
    'by_name')
FORMAT TSVRaw;
SELECT position(
    formatQueryFromJSON(parseQueryToJSON('INSERT INTO insert_by_name_dst_05229 BY NAME SELECT 1 AS a')),
    'BY NAME') > 0
FORMAT TSVRaw;

INSERT INTO insert_by_name_dst_05229 BY NAME SELECT 1 AS does_not_exist_05229; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
INSERT INTO insert_by_name_dst_05229 BY NAME SELECT 1 AS a, 2 AS a; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
INSERT INTO insert_by_name_dst_05229 (a) BY NAME SELECT 1; -- { clientError SYNTAX_ERROR }
INSERT INTO insert_by_name_dst_05229 BY NAME VALUES (1, 'x'); -- { clientError SYNTAX_ERROR }

DROP TABLE insert_by_name_src_05229;
DROP TABLE insert_by_name_input_dst_05229;
DROP TABLE insert_by_name_dst_05229;
