-- `<=>` on `Nullable(Float*)` join keys, split into `isNull` and a non-Nullable key, matches `NaN` and signed zeros
-- as the `tuple` lowering does: the hash table compares the keys bitwise.

SET join_algorithm = 'hash';

DROP TABLE IF EXISTS fl;
DROP TABLE IF EXISTS fr;

CREATE TABLE fl (id UInt32, f Nullable(Float64), g Nullable(Float32)) ENGINE = Memory;
CREATE TABLE fr (id UInt32, f Nullable(Float64), g Nullable(Float32)) ENGINE = Memory;

-- The last row is a `NaN` with another payload.
INSERT INTO fl VALUES (1, NULL, NULL), (2, nan, nan), (3, -nan, -nan), (4, 0., 0.), (5, -0., -0.), (6, 1.5, 1.5),
    (7, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat32(toUInt32(2143289345)));
INSERT INTO fr SELECT id + 10, f, g FROM fl;

SELECT 'Float64', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON l.f <=> r.f ORDER BY a, b);
SELECT 'Float64, tuple', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON tuple(l.f) = tuple(r.f) ORDER BY a, b);

SELECT 'Float32', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON l.g <=> r.g ORDER BY a, b);
SELECT 'Float32, tuple', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON tuple(l.g) = tuple(r.g) ORDER BY a, b);

SELECT 'Float64 and Float32', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON l.f <=> r.g ORDER BY a, b);
SELECT 'Float64 and Float32, tuple', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON tuple(l.f) = tuple(r.g) ORDER BY a, b);

SELECT 'two keys', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON l.f <=> r.f AND l.g <=> r.g ORDER BY a, b);
SELECT 'two keys, tuple', groupArray((a, b)) FROM (SELECT l.id AS a, r.id AS b FROM fl AS l JOIN fr AS r ON tuple(l.f, l.g) = tuple(r.f, r.g) ORDER BY a, b);

DROP TABLE fl;
DROP TABLE fr;
