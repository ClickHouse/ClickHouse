-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

-- Each case is one block of rows, so rows that ICU maps sit between rows that it does not.

-- Two-byte characters: context-dependent (final sigma), length-changing, and after a row that expands.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['ΑΣ', 'ΣΑ', 'ΑΣΑ', 'ПРИВЕТ мир', 'ǅ', 'ı', 'ŉ', 'İé', 'ÉÉ', 'Ab', 'é']) AS s)
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

-- A two-byte sequence split between two rows is two invalid bytes.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['A\xC3', '\xA9b', 'é', 'x\xC3', '\x89', 'Ab', '\xC3', '\xA9']) AS s)
FORMAT TSV;

-- Final sigma depends only on the characters of its own row.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s)))
FROM (SELECT arrayJoin(['Α', 'Σ', 'ΑΣ', 'Α', 'Ж\'', 'Σ', 'ΑΣ\'', 'Ab']) AS s)
FORMAT TSV;

-- Empty, ASCII, table-mapped and ICU rows of every length change, ending with a row that expands.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM
(
    SELECT arrayJoin(['', 'Ab', 'é', 'ÉÉ', '', 'İ', 'Ab', 'İé', 'Ⱥ', 'xK', 'K', 'ẞ', 'Ab', 'ŉ', 'ΐ', 'ΑΣ', '€', 'Ab',
                      '東京', 'Straße', '\xE2', 'ab', 'AB', '', '', 'éé', 'Ⱥé', 'KÉ', 'ẞé', 'ŉé', 'ΐé', 'ΑΣé', 'é€', 'É東',
                      'ab€cd', 'ÉΣ', 'Σ', 'ΣΣ', 'aΣ', 'aΣa', 'abc\xC3', 'abcdé', 'abcdéfgh', 'İİ', 'ŉŉ', 'ΐΐ', 'KK', 'ẞẞ',
                      'abcde', 'É', 'xyz', 'ÀÁÂ', 'àáâ', 'Ж', 'ж', 'ПРИВЕТ', 'привет', 'xé', 'Straße', 'İŉ']) AS s
)
FORMAT TSV;

-- Columns whose only non-ASCII byte is the last one, or the first one.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['Ab', 'Cd', 'Ef\xC3']) AS s)
FORMAT TSV;
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['Ab', 'Cd\xA9']) AS s)
FORMAT TSV;
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['Éb', 'Cd', 'Ef']) AS s)
FORMAT TSV;

-- Non-ASCII characters at every position of the 64-byte blocks of the ASCII scan, and in a stretch of more than 64 KiB.
SELECT sum(cityHash64(i, lowerUTF8(s))), sum(cityHash64(i, upperUTF8(s)))
FROM
(
    SELECT arrayJoin(arrayMap(i -> (i, if(i < 1000,
        concat(repeat('a', i % 150), 'É', repeat('b', i % 70), 'é', repeat('c', (i * 7) % 130)),
        concat('é', repeat('a', 10), 'é', repeat('a', 70000), 'É', 'x'))), range(1001))) AS t,
        t.1 AS i,
        t.2 AS s
);

-- Rows after a row whose ICU output reallocates the result: the first row for upperUTF8, the seventh for lowerUTF8.
SELECT i, length(lowerUTF8(s)), cityHash64(lowerUTF8(s)), length(upperUTF8(s)), cityHash64(upperUTF8(s))
FROM
(
    SELECT arrayJoin(arrayZip(range(1, 11), [repeat('ΐ', 20000), 'Ab', 'abcdefghijklmnopqrstuvwxyzABCDEF', 'É', 'Жж', '€',
        repeat('İ', 40000), 'ÉÉ Ab', 'abcdefghijklmnopqrstuvwxyzABCDEF', 'xé'])) AS t,
        t.1 AS i,
        t.2 AS s
)
FORMAT TSV;

-- A two-byte lead byte followed in its row by a byte that is not a continuation byte is invalid.
SELECT concat('0x', hex(s)), concat('0x', hex(lowerUTF8(s))), concat('0x', hex(upperUTF8(s)))
FROM (SELECT arrayJoin(['Ж\xC3A', '\xC3A', 'é\xC3\xC3\xA9', 'Ab\xDFx', '\xC2 ', 'Ab']) AS s)
FORMAT TSV;
