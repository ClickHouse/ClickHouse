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

-- Every ASCII and two-byte code point in several contexts maps as in a row starting with '€', which goes to ICU whole.
SELECT
    countIf(lowerUTF8(s) != substring(lowerUTF8(concat('€', s)), 4))
    + countIf(upperUTF8(s) != substring(upperUTF8(concat('€', s)), 4))
FROM
(
    SELECT arrayJoin([c, concat('A', c), concat(c, 'A'), concat('A', c, 'A'), concat('Ab', c, c, 'Cd'),
                      concat(c, 'Σ'), concat('A', c, 'Σ'), concat('Ж', c, c, 'Σ')]) AS s
    FROM
    (
        SELECT if(number < 0x80, char(number), char(bitOr(0xC0, bitShiftRight(number, 6)), bitOr(0x80, bitAnd(number, 0x3F)))) AS c
        FROM numbers(0x800)
    )
);

-- Rows the table maps up to a character it has no entry for, then ICU maps the rest.
SELECT
    countIf(lowerUTF8(s) != substring(lowerUTF8(concat('€', s)), 4))
    + countIf(upperUTF8(s) != substring(upperUTF8(concat('€', s)), 4))
FROM
(
    SELECT arrayJoin([concat('Жж', t, 'жЖ'), concat('Жж', t, 'ΣЖ'), concat('Жж\'', t, 'Σ'), concat('Жж\xCC\x81', t, 'Σ'),
                      concat('Жж ', t, 'Σ'), concat('Ab', t, 'Σ'), concat('Ж', t), concat('Ж\'', t)]) AS s
    FROM
    (
        SELECT multiIf(
            number < 0x800, char(bitOr(0xC0, bitShiftRight(number, 6)), bitOr(0x80, bitAnd(number, 0x3F))),
            char(bitOr(0xE0, bitShiftRight(number, 12)), bitOr(0x80, bitAnd(bitShiftRight(number, 6), 0x3F)), bitOr(0x80, bitAnd(number, 0x3F)))) AS t
        FROM numbers(0x80, 0x10000 - 0x80)
        WHERE number < 0xD800 OR number > 0xDFFF
        UNION ALL
        SELECT arrayJoin(['𐐀', '𐐨', '𞤀', '𞤢', '😀', '\xE2', '\xC0\xAF']) AS t
    )
);

DROP TABLE lower_upper_utf8_mixed_ascii;
