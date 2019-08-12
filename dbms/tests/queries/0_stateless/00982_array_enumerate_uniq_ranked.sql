DROP TABLE IF EXISTS array_enumerate_uniq_ranked;
CREATE TABLE array_enumerate_uniq_ranked (x Array(Array(String))) Engine=Memory;
INSERT INTO array_enumerate_uniq_ranked VALUES ([[]]), ([['a'], ['a'], ['b']]), ([['a'], ['a'], ['b']]);
SELECT arrayEnumerateUniqRanked(x, 2) FROM array_enumerate_uniq_ranked;
DROP TABLE array_enumerate_uniq_ranked;