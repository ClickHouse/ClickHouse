SELECT cityHash64([(1, 'a'), (2, 'b')]);
SELECT cityHash64([(1, 'c'), (2, 'b')]);
SELECT sipHash64([(1, 'a'), (2, 'b')]);
SELECT murmurHash2_64([(1, 'a'), (2, 'b'), (3, 'c')]);

SELECT cityHash64([(1, [(1, (3, 4, [(5, 6), (7, 8)]))]), (2, [])] AS c), toTypeName(c);
