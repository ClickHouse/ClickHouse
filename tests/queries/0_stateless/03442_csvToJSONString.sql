DROP TABLE IF EXISTS csv_to_json_test;

----------------------------------------------------------------------------------------------
-- first test with non-nullable String data
----------------------------------------------------------------------------------------------
CREATE TABLE csv_to_json_test (entry Int32, csv String) ENGINE = Memory;

INSERT INTO csv_to_json_test
VALUES
    (1, 'Key1,42,John'),
    (2, 'Key2,true,Bob'),
    (3, 'Key3,false,Alice'),
    (4, 'Key4,something,Charlie'),
    (5, 'Key5|17|Rudolf'),
    (6, '"The,key,with""quotes","\\N",",;''"');

-- with default and with SETTINGS override
SELECT 1 as T, csvToJSONString ('id,property,name', csv) AS json FROM csv_to_json_test WHERE entry < 5 ORDER BY entry;
SELECT 2 as T, csvToJSONString ('id|property|name', csv, 'delimiter="|"') AS json FROM csv_to_json_test WHERE entry == 5;
SELECT 3 as T, csvToJSONString ('id|property|name', csv, 'delimiter="|",detectTypes=true') AS json FROM csv_to_json_test WHERE entry == 5;
SELECT 4 as T, csvToJSONString ('id|property|name', csv, 'delimiter="|",detectTypes=false') AS json FROM csv_to_json_test WHERE entry == 5;

-- skip fields in the output
SELECT 5 as T, csvToJSONString (',property,name', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;
SELECT 6 as T, csvToJSONString ('id,,name', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;
SELECT 7 as T, csvToJSONString ('id,property', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;

-- really messed-up but valid CSV content
SELECT 8 as T, csvToJSONString ('id,property,name', csv) AS json FROM csv_to_json_test WHERE entry == 6;
SELECT 9 as T, csvToJSONString ('f1,f2,f3,f4,f5', csv) AS json FROM csv_to_json_test WHERE entry == 3;
SELECT 10 as T, csvToJSONString ('id,property,name', '') AS json;
SELECT 11 as T, csvToJSONString ('id,property,name', '3,true,"end quote missing') AS json;

-- empty (not null) CSV fields are empty JSON strings (here property)
SELECT 12 as T, csvToJSONString ('id,property,name', 'foo,,bar') AS json;

-- should be just "String" (output is non-nullable if input is non-nullable)
SELECT 13 as T, toTypeName(csvToJSONString ('id,property,name', csv)) AS type FROM csv_to_json_test LIMIT 1;

DROP TABLE csv_to_json_test;

----------------------------------------------------------------------------------------------
-- second, try with nullable string input
----------------------------------------------------------------------------------------------
CREATE TABLE csv_to_json_test_nullable (entry Int32, csv Nullable(String)) ENGINE = Memory;

INSERT INTO csv_to_json_test_nullable
VALUES
    (1, 'Field1.0,Field1.1,\\N'),
    (2, 'Field2.0,\\N,Field2.2'),
    (3, '\\N,Field3.1,Field3.2'),
    (4, NULL);

-- ensure that null-values in CSV (\\N) are detected as well as full CSV can be NULL
SELECT 14 as T, csvToJSONString('col1,col2,col3', csv) AS JSON from csv_to_json_test_nullable;
SELECT 15 as T, csvToJSONString('c1,c2', NULL) AS JSON;

-- should be "Nullable(String)" (output is nullable if input is nullable)
SELECT 16 as T, toTypeName(csvToJSONString ('id,property,name', csv)) AS type FROM csv_to_json_test_nullable LIMIT 1;

DROP TABLE csv_to_json_test_nullable;

----------------------------------------------------------------------------------------------
-- third, more on options
----------------------------------------------------------------------------------------------
SELECT 17 as T, csvToJSONString ('col1!col2!col3!col4', 
                                 '$fiel1$!$field$$2$!42!NIL', 
                                 'delimiter="!",custom_quotes="$",format_csv_null_representation="NIL"') AS json;
