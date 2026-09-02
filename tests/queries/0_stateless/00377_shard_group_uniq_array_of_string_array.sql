-- Tags: shard, long
SET max_rows_to_read = '55M';

DROP TABLE IF EXISTS group_uniq_arr_str;
CREATE TABLE group_uniq_arr_str ENGINE = Memory AS
    SELECT hex(intHash32(g)) as id, if(c == 0, [hex(v)], if(c == 1, emptyArrayString(), [hex(v), hex(v)])) as v FROM
        (SELECT intDiv(number%1000000, 100) as v, intDiv(number%100, 10) as g, number%10 as c FROM system.numbers WHERE c < 3 LIMIT 10000000);

SELECT length(groupUniqArray(v)) FROM group_uniq_arr_str GROUP BY id ORDER BY id;
SELECT length(groupUniqArray(v)) FROM remote('127.0.0.{2,3,4,5}', currentDatabase(), 'group_uniq_arr_str') GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS group_uniq_arr_str;
