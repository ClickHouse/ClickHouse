drop table if exists table;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE table (a UInt32,  date Date, b UInt64,  c UInt64, str String, d Int8, arr Array(UInt64), arr_alias Array(UInt64) ALIAS arr) ENGINE = MergeTree(date, intHash32(c), (a, date, intHash32(c), b), 8192);

SELECT alias2 AS alias3
FROM table 
ARRAY JOIN
    arr_alias AS alias2, 
    arrayEnumerateUniq(arr_alias) AS _uniq_Event
WHERE (date = toDate('2010-10-10')) AND (a IN (2, 3)) AND (str NOT IN ('z', 'x')) AND (d != -1)
LIMIT 1;

drop table if exists table;

