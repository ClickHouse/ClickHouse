-- OUTER/SEMI/ANTI IEJoin with the default `join_use_nulls = 0`: unmatched rows are padded
-- with the default values of the other side's types (0 for numbers, '' for strings, NULL only
-- for columns that are Nullable themselves). The band condition `l.k > r.lo AND l.k < r.hi`
-- is crafted so that the match set equals that of the equality join `l.k = r.mid`
-- (lo = mid - 1, hi = mid + 1, and all keys are multiples of 10), so every result is
-- cross-checked against a hash join of the same kind.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS lpad;
DROP TABLE IF EXISTS rpad;

CREATE TABLE lpad (id Int32, k Nullable(Int32), s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE rpad (id Int32, lo Nullable(Int32), hi Nullable(Int32), mid Nullable(Int32), s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO lpad VALUES
    (1, 10, 'l1'),
    (2, 20, 'l2a'),
    (3, 20, 'l2b'),
    (4, 30, 'l3'),
    (5, 40, 'l4'),
    (6, NULL, 'lnull');

INSERT INTO rpad VALUES
    (1, 9, 11, 10, 'r1'),
    (2, 19, 21, 20, 'r2'),
    (3, 29, 31, 30, 'r3a'),
    (4, 29, 31, 30, 'r3b'),
    (5, 49, 51, 50, 'r5'),
    (6, NULL, NULL, NULL, 'rnull');

SELECT 'left';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l LEFT JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT JOIN rpad r ON l.k = r.mid);

SELECT 'right';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT JOIN rpad r ON l.k = r.mid);

SELECT 'full';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l FULL JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l FULL JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l FULL JOIN rpad r ON l.k = r.mid);

-- SEMI carries values from one arbitrary matching right row: project only the right columns
-- that are identical across a group's rows (lo, hi) so the result is deterministic
SELECT 'semi';
SELECT l.id, l.k, l.s, r.lo, r.hi FROM lpad l LEFT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.lo, r.hi))) FROM lpad l LEFT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.lo, r.hi))) FROM lpad l LEFT SEMI JOIN rpad r ON l.k = r.mid);

SELECT 'anti';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l LEFT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT ANTI JOIN rpad r ON l.k = r.mid);

SELECT 'right semi';
SELECT l.k, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.k, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.k, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT SEMI JOIN rpad r ON l.k = r.mid);

SELECT 'right anti';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT ANTI JOIN rpad r ON l.k = r.mid);

DROP TABLE lpad;
DROP TABLE rpad;

-- `Enum` columns, also inside a `Tuple`, are padded with the type default (the smallest value), not the column zero 0.
DROP TABLE IF EXISTS lpad_enum;
DROP TABLE IF EXISTS rpad_enum;

CREATE TABLE lpad_enum (id Int32, k Int32, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum16('x' = -5, 'y' = 7))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE rpad_enum (id Int32, lo Int32, hi Int32, mid Int32, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum16('x' = -5, 'y' = 7))) ENGINE = MergeTree ORDER BY id;

INSERT INTO lpad_enum VALUES (1, 10, 'b', ('l1', 'y')), (2, 40, 'b', ('l2', 'y'));
INSERT INTO rpad_enum VALUES (1, 9, 11, 10, 'b', ('r1', 'y')), (2, 49, 51, 50, 'b', ('r2', 'y'));

SELECT 'enum left';
SELECT l.id, l.e, l.t, r.id, r.e, r.t FROM lpad_enum l LEFT JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l LEFT JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l LEFT JOIN rpad_enum r ON l.k = r.mid);

SELECT 'enum right';
SELECT l.id, l.e, l.t, r.id, r.e, r.t FROM lpad_enum l RIGHT JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l RIGHT JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l RIGHT JOIN rpad_enum r ON l.k = r.mid);

SELECT 'enum full';
SELECT l.id, l.e, l.t, r.id, r.e, r.t FROM lpad_enum l FULL JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l FULL JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l FULL JOIN rpad_enum r ON l.k = r.mid);

SELECT 'enum anti';
SELECT l.id, l.e, l.t, r.id, r.e, r.t FROM lpad_enum l LEFT ANTI JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l LEFT ANTI JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l LEFT ANTI JOIN rpad_enum r ON l.k = r.mid);

SELECT 'enum right anti';
SELECT l.id, l.e, l.t, r.id, r.e, r.t FROM lpad_enum l RIGHT ANTI JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l RIGHT ANTI JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.e, l.t, r.id, r.e, r.t))) FROM lpad_enum l RIGHT ANTI JOIN rpad_enum r ON l.k = r.mid);

SELECT 'enum filter';
SELECT (SELECT count() FROM lpad_enum l LEFT JOIN rpad_enum r ON l.k > r.lo AND l.k < r.hi WHERE r.e = 'a' AND r.t.e = 'x'),
       (SELECT count() FROM lpad_enum l LEFT JOIN rpad_enum r ON l.k = r.mid WHERE r.e = 'a' AND r.t.e = 'x');

-- A typed `JSON` path of an `Enum` type is padded with the type default as well.
DROP TABLE IF EXISTS rpad_json;
CREATE TABLE rpad_json (id Int32, lo Int32, hi Int32, mid Int32, j JSON(p Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO rpad_json VALUES (1, 9, 11, 10, '{"p":"b"}'), (2, 49, 51, 50, '{"p":"b"}');

SELECT 'enum json';
SELECT l.id, r.id, r.j FROM lpad_enum l LEFT JOIN rpad_json r ON l.k > r.lo AND l.k < r.hi ORDER BY l.id, r.id;
SELECT (SELECT arraySort(groupArray((l.id, r.id, toString(r.j)))) FROM lpad_enum l LEFT JOIN rpad_json r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, r.id, toString(r.j)))) FROM lpad_enum l LEFT JOIN rpad_json r ON l.k = r.mid);
SELECT (SELECT count() FROM lpad_enum l LEFT JOIN rpad_json r ON l.k > r.lo AND l.k < r.hi WHERE r.j.p = 'a'),
       (SELECT count() FROM lpad_enum l LEFT JOIN rpad_json r ON l.k = r.mid WHERE r.j.p = 'a');

DROP TABLE rpad_json;

DROP TABLE lpad_enum;
DROP TABLE rpad_enum;
