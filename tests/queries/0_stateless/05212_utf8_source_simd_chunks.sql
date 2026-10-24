SELECT throwIf(
    (id = 1 AND (
        hex(leftUTF8(materialize(s), 32)) != repeat('61', 32)
        OR hex(rightUTF8(materialize(s), 32)) != repeat('61', 32)
        OR hex(substringUTF8(materialize(s), 33, 2)) != repeat('61', 2)))
    OR (id = 2 AND (
        hex(leftUTF8(materialize(s), 32)) != concat(repeat('61', 31), 'C3A9')
        OR hex(rightUTF8(materialize(s), 32)) != repeat('62', 32)
        OR hex(substringUTF8(materialize(s), 33, 2)) != repeat('62', 2)))
    OR (id = 3 AND (
        hex(leftUTF8(materialize(s), 32)) != repeat('61', 32)
        OR hex(rightUTF8(materialize(s), 32)) != concat('C3A9', repeat('62', 31))
        OR hex(substringUTF8(materialize(s), 33, 2)) != concat('C3A9', '62')))
    OR (id = 4 AND (
        hex(leftUTF8(materialize(s), 32)) != concat(repeat('61', 16), 'C3A9', repeat('62', 15))
        OR hex(rightUTF8(materialize(s), 32)) != repeat('62', 32)
        OR hex(substringUTF8(materialize(s), 33, 2)) != repeat('62', 2)))
    OR (id = 5 AND (
        hex(leftUTF8(materialize(s), 32)) != ''
        OR hex(rightUTF8(materialize(s), 32)) != ''
        OR hex(substringUTF8(materialize(s), 33, 2)) != ''))
    OR (id = 6 AND (
        hex(leftUTF8(materialize(s), 32)) != concat(repeat('61', 31), '80')
        OR hex(rightUTF8(materialize(s), 32)) != repeat('62', 32)
        OR hex(substringUTF8(materialize(s), 33, 2)) != repeat('62', 2)))
    , 'UTF-8 source chunk traversal mismatch')
FROM
(
    SELECT 1 AS id, repeat('a', 64) AS s
    UNION ALL
    SELECT 2, concat(repeat('a', 31), unhex('C3A9'), repeat('b', 47))
    UNION ALL
    SELECT 3, concat(repeat('a', 32), unhex('C3A9'), repeat('b', 31))
    UNION ALL
    SELECT 4, concat(repeat('a', 16), unhex('C3A9'), repeat('b', 48))
    UNION ALL
    SELECT 5, ''
    UNION ALL
    SELECT 6, concat(repeat('a', 31), unhex('80'), repeat('b', 47))
)
FORMAT Null;

-- Exercise word and chunk boundaries, short requests, mixed chunks, and clipped negative offsets.
-- Include non-aligned multi-chunk requests to exercise the final partial ASCII chunk.
-- Construct expected results from code-point arrays, independently of UTF-8 string traversal.
WITH
    arrayConcat(arrayMap(x -> 'a', range(prefix)), [code_point], arrayMap(x -> 'b', range(suffix))) AS code_points,
    arrayStringConcat(code_points) AS s,
    length(code_points) AS code_point_count,
    least(skip, code_point_count) AS right_size
SELECT throwIf(
    leftUTF8(materialize(s), skip) != arrayStringConcat(arraySlice(code_points, 1, skip))
    OR rightUTF8(materialize(s), skip) != arrayStringConcat(arraySlice(code_points, code_point_count - right_size + 1, right_size))
    OR substringUTF8(materialize(s), -toInt64(code_point_count + 5), 8) != arrayStringConcat(arraySlice(code_points, 1, 3))
    OR substringUTF8(materialize(s), -toInt64(code_point_count + 5), 5) != ''
    , 'UTF-8 source short request or clipped offset mismatch')
FROM
    (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 64]) AS prefix) AS prefixes
CROSS JOIN
    (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 64]) AS suffix) AS suffixes
CROSS JOIN
    (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 17, 24, 31, 32, 33, 39, 40, 41, 47, 48, 49, 55, 56, 57, 63, 64, 65, 71, 72, 73, 95, 96, 97, 127, 128, 129]) AS skip) AS skips
CROSS JOIN
    (SELECT arrayJoin([unhex('00'), 'a', unhex('C3A9'), unhex('E4BDA0'), unhex('F09F9880')]) AS code_point) AS code_points_source
FORMAT Null;

-- Short ASCII strings exercise the word fallback when a full chunk would cross a row boundary.
-- Byte-oriented operations provide an independent oracle, including unaligned substring starts.
WITH repeat('a', byte_length) AS s
SELECT throwIf(
    leftUTF8(materialize(s), skip) != repeat('a', least(byte_length, skip))
    OR rightUTF8(materialize(s), skip) != repeat('a', least(byte_length, skip))
    OR substringUTF8(materialize(s), byte_offset + 1, skip) != substring(s, byte_offset + 1, skip)
    , 'UTF-8 source ASCII word boundary mismatch')
FROM
    (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65]) AS byte_length) AS lengths
CROSS JOIN
    (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 129]) AS skip) AS skips
CROSS JOIN
    (SELECT arrayJoin([0, 1, 7, 8, 9, 31, 32, 33]) AS byte_offset) AS byte_offsets
FORMAT Null;

SELECT 'OK';
