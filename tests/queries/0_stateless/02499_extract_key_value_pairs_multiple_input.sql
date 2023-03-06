-- { echoOn }
----- Enclosed elements tests -----

-- Expected output: {'name':'Neymar','team':'psg','age':'30','favorite_movie':'','height':'1.75'}
WITH
    extractKeyValuePairs('"name": "Neymar", "age": 30, team: "psg", "favorite_movie": "", height: 1.75', ':', ',', '"', '.') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

----- Escaping tests -----

-- Expected output: {'age':'30'}
WITH
    extractKeyValuePairs('na,me,: neymar, age:30', ':', ',', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- Expected output: {'age':'30'}
WITH
    extractKeyValuePairs('na$me,: neymar, age:30', ':', ',', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- Expected output: {'name':'neymar','favorite_quote':'Premature optimization is the r$$t of all evil','age':'30'}
WITH
    extractKeyValuePairs('name: neymar, favorite_quote: Premature! optimization! is! the! r!$!$t! of! all! evil, age:30', ':', ',', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- Expected output: {'name':'neymar','favorite_quote':'Premature optimization is the root of all evi','age':'30'}
WITH
    extractKeyValuePairs('name: neymar, favorite_quote: Premature!! optimization, age:30', ':', ',', '"') AS s_map,
    CAST(
            arrayMap(
                (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
            ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

----- Mix Strings -----
WITH
    extractKeyValuePairs('9 ads =nm,  na!:me: neymar, age: 30, daojmskdpoa and a  height:   1.75, school: lupe! picasso, team: psg,', ':', ',', '"', '.') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

-- Expected output: {'XNFHGSSF_RHRUZHVBS_KWBT':'F'}
WITH
    extractKeyValuePairs('XNFHGSSF_RHRUZHVBS_KWBT: F,', ':', ',', '"') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;

----- Allow list special value characters -----

-- Expected output: {'some_key':'@dolla%sign$'}
WITH
    extractKeyValuePairs('some_key: @dolla%sign$,', ':', ',', '"', '$@%') AS s_map,
    CAST(
            arrayMap(
                    (x) -> (x, s_map[x]), arraySort(mapKeys(s_map))
                ),
            'Map(String,String)'
        ) AS x
SELECT
    x;
