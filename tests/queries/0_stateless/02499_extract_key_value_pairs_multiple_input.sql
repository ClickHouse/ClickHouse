-- { echoOn }

-- basic tests

-- expected output: {'age':'31','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- keys and values starting with number, underscore and other special characters
-- expected output: {'$nationality':'@brazil','1name':'neymar','4ge':'31','_team':'_psg'}
WITH
    extractKeyValuePairs('1name:neymar, 4ge:31 _team:_psg,$nationality:@brazil') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- only special characters
-- expected output: {'#':'#','$':'$','@':'@','_':'_'}
WITH
    extractKeyValuePairs('_:_, @:@ #:#,$:$') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- special (not control) characters in the middle of elements
-- expected output: {'age':'3!','name':'ney!mar','nationality':'br4z!l','t&am':'@psg'}
WITH
    extractKeyValuePairs('name:ney!mar, age:3! t&am:@psg,nationality:br4z!l') AS s_map,
        CAST(
            arrayMap(
                (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
            ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- non-standard escape characters (i.e not \n, \r, \t and etc), back-slash should be preserved
-- expected output: {'amount\\z':'$5\\h','currency':'\\$USD'}
WITH
    extractKeyValuePairs('currency:\$USD, amount\z:$5\h') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- invalid escape sequence at the end of file should be ignored
-- expected output: {'key':'invalid_escape_sequence','valid_key':'valid_value'}
WITH
    extractKeyValuePairsWithEscaping('valid_key:valid_value key:invalid_escape_sequence\\', ':', ' ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- standard escape sequences are covered by unit tests

-- simple quoting
-- expected output: {'age':'31','name':'neymar','team':'psg'}
WITH
    extractKeyValuePairs('name:"neymar", "age":31 "team":"psg"') AS s_map,
        CAST(
            arrayMap(
                (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
            ),
        'Map(String,String)'
    ) AS x
SELECT
    x;

-- empty values
-- expected output: {'age':'','name':'','nationality':''}
WITH
    extractKeyValuePairs('name:"", age: , nationality:') AS s_map,
    CAST(
        arrayMap(
            (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
        ),
        'Map(String,String)'
    ) AS x
SELECT
    x;

-- empty keys
-- empty keys are not allowed, thus empty output is expected
WITH
    extractKeyValuePairs('"":abc, :def') AS s_map,
    CAST(
        arrayMap(
            (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
        ),
        'Map(String,String)'
    ) AS x
SELECT
    x;

-- semi-colon as pair delimiter
-- expected output: {'age':'31','anotherkey':'anothervalue','name':'neymar','random_key':'value_with_comma,still_part_of_value:still_part_of_value','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar;age:31;team:psg;random_key:value_with_comma,still_part_of_value:still_part_of_value;anotherkey:anothervalue', ':', ';') AS s_map,
    CAST(
        arrayMap(
            (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
        ),
        'Map(String,String)'
    ) AS x
SELECT
    x;

-- both comma and semi-colon as pair delimiters
-- expected output: {'age':'31','last_key':'last_value','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar;age:31;team:psg;nationality:brazil,last_key:last_value', ':', ';,') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- single quote as quoting character
-- expected output: {'age':'31','last_key':'last_value','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- NO ESCAPING TESTS
-- expected output: {'age':'31','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- special (not control) characters in the middle of elements
-- expected output: {'age':'3!','name':'ney!mar','nationality':'br4z!l','t&am':'@psg'}
WITH
    extractKeyValuePairs('name:ney!mar, age:3! t&am:@psg,nationality:br4z!l', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- non-standard escape characters (i.e not \n, \r, \t and etc), it should accept everything
-- expected output: {'amount\\z':'$5\\h','currency':'\\$USD'}
WITH
    extractKeyValuePairs('currency:\$USD, amount\z:$5\h', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- standard escape sequences, it should return it as it is
-- expected output: {'key1':'header\nbody','key2':'start_of_text\tend_of_text'}
WITH
    extractKeyValuePairs('key1:header\nbody key2:start_of_text\tend_of_text', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- standard escape sequences are covered by unit tests

-- simple quoting
-- expected output: {'age':'31','name':'neymar','team':'psg'}
WITH
    extractKeyValuePairs('name:"neymar", "age":31 "team":"psg"', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- empty values
-- expected output: {'age':'','name':'','nationality':''}
WITH
    extractKeyValuePairs('name:"", age: , nationality:', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- empty keys
-- empty keys are not allowed, thus empty output is expected
WITH
    extractKeyValuePairs('"":abc, :def', ':', ', ', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- semi-colon as pair delimiter
-- expected output: {'age':'31','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar;age:31;team:psg;nationality:brazil', ':', ';', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- both comma and semi-colon as pair delimiters
-- expected output: {'age':'31','last_key':'last_value','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:neymar;age:31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- single quote as quoting character
-- expected output: {'age':'31','last_key':'last_value','name':'neymar','nationality':'brazil','team':'psg'}
WITH
    extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- { echoOff }

-- cross parameter validation tests
-- should fail because key value delimiter conflicts with pair delimiters
WITH
    extractKeyValuePairs('not_important', ':', ',:', '\'') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError BAD_ARGUMENTS}

-- should fail because key value delimiter conflicts with quoting characters
WITH
    extractKeyValuePairs('not_important', ':', ',', '\':') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError BAD_ARGUMENTS}

-- should fail because pair delimiters conflicts with quoting characters
WITH
    extractKeyValuePairs('not_important', ':', ',', ',') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError BAD_ARGUMENTS}

-- should fail because data_column argument must be of type String
WITH
    extractKeyValuePairs([1, 2]) AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- should fail because key_value_delimiter argument must be of type String
WITH
    extractKeyValuePairs('', [1, 2]) AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- should fail because pair_delimiters argument must be of type String
WITH
    extractKeyValuePairs('', ':', [1, 2]) AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- should fail because quoting_character argument must be of type String
WITH
    extractKeyValuePairs('', ':', ' ', [1, 2]) AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- should fail because pair delimiters can contain at most 8 characters
WITH
    extractKeyValuePairs('not_important', ':', '123456789', '\'') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError BAD_ARGUMENTS}

-- should fail because no argument has been provided
WITH
    extractKeyValuePairs() AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

-- should fail because one extra argument / non existent has been provided
WITH
    extractKeyValuePairs('a', ':', ',', '"', '') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

-- Should fail allowed because it exceeds the max number of pairs
SET extract_key_value_pairs_max_pairs_per_row = 1;
WITH
    extractKeyValuePairs('key1:value1,key2:value2') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x; -- {serverError LIMIT_EXCEEDED}

-- { echoOn }

SET extract_key_value_pairs_max_pairs_per_row = 2;
-- Should be allowed because it no longer exceeds the max number of pairs
-- expected output: {'key1':'value1','key2':'value2'}
WITH
    extractKeyValuePairs('key1:value1,key2:value2') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

SET extract_key_value_pairs_max_pairs_per_row = 0;
-- Should be allowed because max pairs per row is set to 0 (unlimited)
-- expected output: {'key1':'value1','key2':'value2'}
WITH
    extractKeyValuePairs('key1:value1,key2:value2') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- should not fail because pair delimiters contains 8 characters, which is within the limit
WITH
    extractKeyValuePairs('not_important', ':', '12345678', '\'') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- key value delimiter should be considered valid part of value
WITH
    extractKeyValuePairs('formula=1+2=3 argument1=1 argument2=2 result=3, char="=" char2== string="foo=bar"', '=') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- https://github.com/ClickHouse/ClickHouse/issues/56357
WITH
    extractKeyValuePairs('{"a":"1", "b":"2"}') as s_map,
    CAST(
        arrayMap(
            (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
        ),
        'Map(String,String)'
    ) AS x
SELECT
    x;

-- check str_to_map alias (it is case-insensitive)
WITH
    sTr_tO_mAp('name:neymar, age:31 team:psg,nationality:brazil') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- check mapFromString alias
WITH
    mapFromString('name:neymar, age:31 team:psg,nationality:brazil') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;
