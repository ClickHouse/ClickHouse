CREATE TABLE array_join_reuse (id UInt8, arrays Array(Array(Int64)), labels Array(String)) ENGINE = Memory;
INSERT INTO array_join_reuse VALUES (1, [[1, 2], [], [3]], ['a', 'b', 'c']), (2, [[], [4, 5]], ['d', 'e']), (3, [], []);

SELECT id, arrays, item, arrayReverse(item), label FROM array_join_reuse
ARRAY JOIN arrays AS item, labels AS label WHERE notEmpty(item) ORDER BY id, label
SETTINGS max_block_size = 65536, query_plan_fuse_filter_into_array_join = 1;

SELECT id, arrays, item, arrayReverse(item), label FROM array_join_reuse
ARRAY JOIN arrays AS item, labels AS label WHERE notEmpty(item) ORDER BY id, label
SETTINGS max_block_size = 2, query_plan_fuse_filter_into_array_join = 0;

SELECT id, arrays, item, toJSONString(label) FROM array_join_reuse
LEFT ARRAY JOIN arrays AS item, labels AS label ORDER BY id, label;

DROP TABLE array_join_reuse;
