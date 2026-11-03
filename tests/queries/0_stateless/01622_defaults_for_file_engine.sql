insert into table function file("data1622.json", "TSV", "value String") VALUES ('{"a":1}');
drop table if exists json;
create table json(a int, b int default 7, c default a + b) engine File(JSONEachRow, 'data1622.json');
set input_format_defaults_for_omitted_fields = 1;
select * from json;
truncate table json;
drop table if exists json;

-- https://github.com/ClickHouse/ClickHouse/issues/121905
-- Reading only "b" also reads "a" for its DEFAULT.
INSERT INTO FUNCTION file(currentDatabase() || '_01622.json', JSONColumns, 'a UInt32, b Nullable(UInt32)')
    SELECT * FROM values('a UInt32, b Nullable(UInt32)', (6, NULL), (7, 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_01622.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a*2');
SELECT c FROM file(currentDatabase() || '_01622.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a*2, c UInt32 DEFAULT b + 1');

-- Without input_format_defaults_for_omitted_fields the columns of DEFAULT expressions are not read (here "a" cannot be parsed).
INSERT INTO FUNCTION file(currentDatabase() || '_01622_unparsable.json', JSONColumns, 'a String, b UInt32')
    SELECT * FROM values('a String, b UInt32', ('oops', 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_01622_unparsable.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a * 2')
    SETTINGS input_format_defaults_for_omitted_fields = 0;

-- The default of a column which is read only for a DEFAULT is computed only if that DEFAULT is computed:
-- "b" is present, so the default of "c" (which fails for "oops") is not needed.
SELECT b FROM file(currentDatabase() || '_01622_unparsable.json', JSONColumns, 'a String, c UInt32 DEFAULT toUInt32(a), b UInt32 DEFAULT c * 2');

-- A missing tuple, which a DEFAULT uses as "a.x", takes its own default first: the same with and without "a.x" selected.
INSERT INTO FUNCTION file(currentDatabase() || '_01622_tuple_default.json', JSONColumns, 'z UInt32, a Nullable(UInt32), b Nullable(UInt32)')
    SELECT * FROM values('z UInt32, a Nullable(UInt32), b Nullable(UInt32)', (4, NULL, NULL)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_01622_tuple_default.json', JSONColumns, 'z UInt32, a Tuple(x UInt32) DEFAULT tuple(z * 2), b UInt32 DEFAULT a.x');
SELECT a.x, b FROM file(currentDatabase() || '_01622_tuple_default.json', JSONColumns, 'z UInt32, a Tuple(x UInt32) DEFAULT tuple(z * 2), b UInt32 DEFAULT a.x');

-- "c" is present, so the default of "d" (which fails for "oops") is not needed for the missing "b".
INSERT INTO FUNCTION file(currentDatabase() || '_01622_present_dependency.json', JSONColumns, 'a String, c UInt32, b Nullable(UInt32)')
    SELECT * FROM values('a String, c UInt32, b Nullable(UInt32)', ('oops', 7, NULL)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_01622_present_dependency.json', JSONColumns, 'a String, d UInt32 DEFAULT toUInt32(a), c UInt32 DEFAULT d, b UInt32 DEFAULT c * 2');

-- "a.x" of the lambda argument "a" is not the column "a": it is not read, and its default (which fails for "oops") is not computed.
INSERT INTO FUNCTION file(currentDatabase() || '_01622_lambda_subcolumn.jsonl', LineAsString, 'line String')
    SETTINGS engine_file_truncate_on_insert = 1 VALUES ('{"z":"oops","b":null}');

SELECT b FROM file(currentDatabase() || '_01622_lambda_subcolumn.jsonl', JSONEachRow,
    'z String, a Tuple(x UInt32) DEFAULT tuple(toUInt32(z)), b UInt32 DEFAULT arraySum(arrayMap(a -> a.x, [CAST(tuple(2) AS Tuple(x UInt32))]))');

-- "a.x" of the alias "a" is not the column "a" either.
SELECT b FROM file(currentDatabase() || '_01622_lambda_subcolumn.jsonl', JSONEachRow,
    'z String, a Tuple(x UInt32) DEFAULT tuple(toUInt32(z)), b UInt32 DEFAULT arraySum(arrayMap(x -> (CAST(tuple(2) AS Tuple(x UInt32)) AS a).x + a.x, [1, 2]))');
