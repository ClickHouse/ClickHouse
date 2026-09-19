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

SELECT 'OK';
