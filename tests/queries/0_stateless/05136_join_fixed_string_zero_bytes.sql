-- The comparison functions treat `FixedString` vs `String` as zero-padded: values that differ only
-- in trailing zero bytes are equal. Join keys must follow the same rule.

DROP TABLE IF EXISTS fs;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS fs5;

CREATE TABLE fs (x FixedString(3), i UInt8) ENGINE = Memory;
INSERT INTO fs VALUES ('a', 1), ('ab', 1), ('a\0b', 1), ('abc', 1), ('', 1);

CREATE TABLE s (y String, j UInt8) ENGINE = Memory;
INSERT INTO s VALUES ('a', 0), ('a\0\0', 0), ('a\0\0\0\0', 0), ('a\0b', 0), ('abcd', 0), ('', 0), ('\0\0\0\0', 0), ('ab', 0), ('ab\0\0', 0), ('abd', 0);

CREATE TABLE fs5 (z FixedString(5), k UInt8) ENGINE = Memory;
INSERT INTO fs5 VALUES ('a', 0), ('a\0b', 0), ('abc', 0), ('abcde', 0), ('', 0);

SELECT '-- oracle: operator on the cross product';
SELECT countIf(x = y), countIf(x < y), countIf(x > y) FROM fs, s;

SELECT '-- equality per algorithm';
SELECT 'hash', count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'hash';
SELECT 'parallel_hash', count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'parallel_hash';
SELECT 'grace_hash', count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'grace_hash';
SELECT 'partial_merge', count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'partial_merge';
SELECT 'full_sorting_merge', count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'swapped sides', count() FROM s JOIN fs ON s.y = fs.x SETTINGS join_algorithm = 'hash';
SELECT 'where', count() FROM fs, s WHERE fs.x = s.y;

SELECT '-- matched pairs keep the original bytes';
SELECT hex(x), hex(y) FROM fs JOIN s ON fs.x = s.y ORDER BY ALL SETTINGS join_algorithm = 'hash';

SELECT '-- inequality per algorithm';
SELECT 'ie_join <', count() FROM fs JOIN s ON fs.x < s.y AND fs.i > s.j SETTINGS join_algorithm = 'ie_join';
SELECT 'ie_join >', count() FROM fs JOIN s ON fs.x > s.y AND fs.i > s.j SETTINGS join_algorithm = 'ie_join';
SELECT 'hash <', count() FROM fs JOIN s ON fs.x < s.y SETTINGS join_algorithm = 'hash';
SELECT 'ie_join < swapped', count() FROM s JOIN fs ON s.y > fs.x AND s.j < fs.i SETTINGS join_algorithm = 'ie_join';

SELECT '-- semi / anti';
SELECT 'oracle semi', count() FROM (SELECT x FROM fs, s GROUP BY x HAVING countIf(x = y) > 0);
SELECT 'semi', count() FROM fs LEFT SEMI JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'hash';
SELECT 'anti', count() FROM fs LEFT ANTI JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'hash';

SELECT '-- LEFT JOIN, unmatched rows';
SELECT hex(x), hex(y) FROM fs LEFT JOIN s ON fs.x = s.y ORDER BY ALL SETTINGS join_algorithm = 'hash';

SELECT '-- Nullable and LowCardinality wrappers';
SELECT countIf(x = y) FROM fs, (SELECT toNullable(y) AS y FROM s) AS sn;
SELECT count() FROM fs JOIN (SELECT toNullable(y) AS y FROM s) AS sn ON fs.x = sn.y SETTINGS join_algorithm = 'hash';
SELECT count() FROM fs JOIN (SELECT toLowCardinality(y) AS y FROM s) AS sl ON fs.x = sl.y SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT toNullable(x) AS x FROM fs) AS fsn JOIN s ON fsn.x = s.y SETTINGS join_algorithm = 'hash';

SELECT '-- FixedString(3) vs FixedString(5)';
SELECT countIf(x = z), countIf(x < z) FROM fs, fs5;
SELECT count() FROM fs JOIN fs5 ON fs.x = fs5.z SETTINGS join_algorithm = 'hash';
SELECT count() FROM fs JOIN fs5 ON fs.x < fs5.z AND fs.i > fs5.k SETTINGS join_algorithm = 'ie_join';

SELECT '-- USING';
SELECT count() FROM fs JOIN (SELECT y AS x FROM s) AS s2 USING (x) SETTINGS join_algorithm = 'hash';

SELECT '-- FULL JOIN USING: the USING column, then the raw key of each side';
SELECT hex(x), hex(fs.x), hex(s2.x) FROM fs FULL JOIN (SELECT y AS x, j FROM s) AS s2 USING (x) ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT hex(x), hex(fs.x), hex(s2.x) FROM fs FULL JOIN (SELECT y AS x, j FROM s) AS s2 USING (x) ORDER BY ALL SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT hex(x), hex(s2.x), hex(fs.x) FROM (SELECT y AS x, j FROM s) AS s2 FULL JOIN fs USING (x) ORDER BY ALL SETTINGS join_algorithm = 'hash';

DROP TABLE fs;
DROP TABLE s;
DROP TABLE fs5;
