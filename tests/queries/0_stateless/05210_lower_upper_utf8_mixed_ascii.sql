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

DROP TABLE lower_upper_utf8_mixed_ascii;
