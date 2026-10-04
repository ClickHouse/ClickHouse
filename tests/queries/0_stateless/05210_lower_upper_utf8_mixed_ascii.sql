-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

DROP TABLE IF EXISTS lower_upper_utf8_mixed_ascii;
CREATE TABLE lower_upper_utf8_mixed_ascii (id UInt8, str String) ENGINE = Memory;

INSERT INTO lower_upper_utf8_mixed_ascii VALUES
    (1, 'ASCII prefix'),
    (2, 'MÜNCHEN'),
    (3, 'ASCII middle'),
    (4, 'Straße'),
    (5, 'ASCII suffix'),
    (6, '東京'),
    (7, ''),
    (8, 'ASCII tail'),
    (9, 'İ'),
    (10, 'ASCII after expansion'),
    (11, '\xe2'),
    (12, 'ASCII after invalid UTF-8'),
    (13, 'K'),
    (14, 'ASCII after contraction'),
    (15, 'é'),
    (16, 'Ab');

SELECT id, concat('0x', hex(lowerUTF8(str))), concat('0x', hex(upperUTF8(str)))
FROM lower_upper_utf8_mixed_ascii
ORDER BY id
FORMAT TSV;

-- Two-byte characters: context-dependent (final sigma), length-changing, and after a row that expands.
INSERT INTO lower_upper_utf8_mixed_ascii VALUES
    (17, 'ΑΣ'),
    (18, 'ΣΑ'),
    (19, 'ΑΣΑ'),
    (20, 'ПРИВЕТ мир'),
    (21, 'ǅ'),
    (22, 'ı'),
    (23, 'ŉ'),
    (24, 'İé'),
    (25, 'ÉÉ'),
    (26, 'Ab'),
    (27, 'é');

SELECT id, concat('0x', hex(lowerUTF8(str))), concat('0x', hex(upperUTF8(str)))
FROM lower_upper_utf8_mixed_ascii
WHERE id >= 17
ORDER BY id
FORMAT TSV;

-- Every code point U+0080..U+07FF in several contexts maps the same as with a trailing '€', which has no case mapping.
SELECT
    countIf(lowerUTF8(s) != substring(lowerUTF8(concat(s, '€')), 1, length(lowerUTF8(concat(s, '€'))) - 3))
    + countIf(upperUTF8(s) != substring(upperUTF8(concat(s, '€')), 1, length(upperUTF8(concat(s, '€'))) - 3))
FROM
(
    SELECT arrayJoin([c, concat('A', c), concat(c, 'A'), concat('A', c, 'A'), concat('Ab', c, c, 'Cd')]) AS s
    FROM
    (
        SELECT char(bitOr(0xC0, bitShiftRight(number, 6)), bitOr(0x80, bitAnd(number, 0x3F))) AS c
        FROM numbers(0x80, 0x780)
    )
);

DROP TABLE lower_upper_utf8_mixed_ascii;
