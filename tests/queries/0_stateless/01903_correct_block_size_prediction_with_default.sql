CREATE TABLE test_extract(str String,  arr Array(Array(String)) ALIAS extractAllGroupsHorizontal(str, '\\W(\\w+)=("[^"]*?"|[^",}]*)')) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY tuple();

INSERT INTO test_extract (str) WITH range(8) as range_arr, arrayMap(x-> concat(toString(x),'Id'), range_arr) as key, arrayMap(x -> rand() % 30, range_arr) as val, arrayStringConcat(arrayMap((x,y) -> concat(x,'=',toString(y)), key, val),',') as str, 1 as test SELECT str FROM numbers(1000000);

ALTER TABLE test_extract ADD COLUMN `15Id` Nullable(UInt16) DEFAULT toUInt16OrNull(arrayFirst((v, k) -> (k = '15Id'), arr[2], arr[1]));

SELECT uniq(15Id) FROM test_extract SETTINGS max_threads=1, max_memory_usage=100000000;

SELECT uniq(15Id) FROM test_extract PREWHERE 15Id < 4 SETTINGS max_threads=1, max_memory_usage=100000000;

SELECT uniq(15Id) FROM test_extract WHERE 15Id < 4 SETTINGS max_threads=1, max_memory_usage=100000000;
