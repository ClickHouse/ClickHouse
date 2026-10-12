-- Case-insensitive UTF-8 match: code points match iff their folds are equal; the fold is `Poco::Unicode::toLower`,
-- except U+212A KELVIN SIGN, U+212B ANGSTROM SIGN, U+2126 OHM SIGN and U+03F4 GREEK CAPITAL THETA SYMBOL fold to themselves,
-- so they match only themselves. U+00B5 MICRO SIGN does not match `μ`, while `ι`/`Ι` and `μ`/`Μ` match.
-- Results must not depend on constness, haystack length or neighbouring rows. Each query prints the shapes
-- const/const, column/const, column/column, const/column. Look-alike characters are written with `char`.

SELECT 'position';
WITH 'ⱥ' AS h, 'Ⱥ' AS n SELECT 'A-bar to a-bar', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'Ⱥ' AS h, 'ⱥ' AS n SELECT 'a-bar to A-bar', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'xⱥy' AS h, 'Ⱥy' AS n SELECT 'A-bar shifted', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'ⱥ' AS h, 'Ⱥb' AS n SELECT 'a-bar needle longer', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'ß' AS h, 'ẞ' AS n SELECT 'sharp s to capital', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'ẞ' AS h, 'ß' AS n SELECT 'capital to sharp s', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'STRAẞE' AS h, 'straße' AS n SELECT 'STRASSE', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'ß' AS h, 'ss' AS n SELECT 'sharp s vs ss', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xE2, 0x84, 0xAA) AS h, 'k' AS n SELECT 'kelvin vs k, first', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat('xx', char(0xE2, 0x84, 0xAA), 'zz') AS h, 'kzz' AS n SELECT 'kelvin vs k, in match', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'xxkzz' AS h, concat(char(0xE2, 0x84, 0xAA), 'zz') AS n SELECT 'k vs kelvin, in needle', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xE2, 0x84, 0xAA) AS h, char(0xE2, 0x84, 0xAA) AS n SELECT 'kelvin to kelvin, first', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat('xx', char(0xE2, 0x84, 0xAA), 'zz') AS h, concat(char(0xE2, 0x84, 0xAA), 'zz') AS n SELECT 'kelvin to kelvin, in match', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat('x', char(0xE2, 0x84, 0xAB)) AS h, 'å' AS n SELECT 'angstrom vs a-ring', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat('x', char(0xE2, 0x84, 0xAB)) AS h, char(0xE2, 0x84, 0xAB) AS n SELECT 'angstrom to angstrom', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xE2, 0x84, 0xA6) AS h, 'ω' AS n SELECT 'ohm vs omega', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xE2, 0x84, 0xA6) AS h, char(0xE2, 0x84, 0xA6) AS n SELECT 'ohm to ohm', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCF, 0xB4) AS h, 'θ' AS n SELECT 'theta symbol vs theta', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCF, 0xB4) AS h, char(0xCF, 0xB4) AS n SELECT 'theta symbol to theta symbol', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCE, 0x99) AS h, char(0xCE, 0xB9) AS n SELECT 'IOTA to iota', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCE, 0xB9) AS h, char(0xCE, 0x99) AS n SELECT 'iota to IOTA', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCE, 0x9C) AS h, char(0xCE, 0xBC) AS n SELECT 'MU to mu', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCE, 0xBC) AS h, char(0xCE, 0x9C) AS n SELECT 'mu to MU', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xC2, 0xB5) AS h, char(0xCE, 0xBC) AS n SELECT 'micro vs mu', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xCE, 0xBC) AS h, char(0xC2, 0xB5) AS n SELECT 'mu vs micro', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH char(0xC2, 0xB5) AS h, char(0xC2, 0xB5) AS n SELECT 'micro to micro', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat(repeat('x', 40), char(0xCE, 0xBC), repeat('x', 40)) AS h, char(0xCE, 0x9C) AS n SELECT 'mu to MU, long', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH concat(repeat('x', 40), char(0xC2, 0xB5), repeat('x', 40)) AS h, char(0xCE, 0xBC) AS n SELECT 'micro vs mu, long', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'Hello World' AS h, 'WORLD' AS n SELECT 'ascii', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'Привет Мир' AS h, 'МИР' AS n SELECT 'cyrillic', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'abc' AS h, '' AS n SELECT 'empty needle', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));
WITH 'ab' AS h, 'abc' AS n SELECT 'needle longer', positionCaseInsensitiveUTF8(h, n), positionCaseInsensitiveUTF8(materialize(h), n), positionCaseInsensitiveUTF8(materialize(h), materialize(n)), positionCaseInsensitiveUTF8(h, materialize(n));

SELECT 'position with start_pos';
WITH concat('x', char(0xE2, 0x84, 0xAA), 'x', char(0xE2, 0x84, 0xAA)) AS h, char(0xE2, 0x84, 0xAA) AS n SELECT 'kelvin to kelvin, skip first', positionCaseInsensitiveUTF8(h, n, 3), positionCaseInsensitiveUTF8(materialize(h), n, 3), positionCaseInsensitiveUTF8(materialize(h), materialize(n), 3), positionCaseInsensitiveUTF8(h, materialize(n), 3), positionCaseInsensitiveUTF8(h, n, materialize(3)), positionCaseInsensitiveUTF8(materialize(h), n, materialize(3)), positionCaseInsensitiveUTF8(materialize(h), materialize(n), materialize(3)), positionCaseInsensitiveUTF8(h, materialize(n), materialize(3));
WITH 'ⱥxⱥ' AS h, 'Ⱥ' AS n SELECT 'a-bar', positionCaseInsensitiveUTF8(h, n, 2), positionCaseInsensitiveUTF8(materialize(h), n, 2), positionCaseInsensitiveUTF8(materialize(h), materialize(n), 2), positionCaseInsensitiveUTF8(h, materialize(n), 2), positionCaseInsensitiveUTF8(h, n, materialize(2)), positionCaseInsensitiveUTF8(materialize(h), n, materialize(2)), positionCaseInsensitiveUTF8(materialize(h), materialize(n), materialize(2)), positionCaseInsensitiveUTF8(h, materialize(n), materialize(2));
WITH 'abcabc' AS h, 'B' AS n SELECT 'ascii', positionCaseInsensitiveUTF8(h, n, 3), positionCaseInsensitiveUTF8(materialize(h), n, 3), positionCaseInsensitiveUTF8(materialize(h), materialize(n), 3), positionCaseInsensitiveUTF8(h, materialize(n), 3), positionCaseInsensitiveUTF8(h, n, materialize(3)), positionCaseInsensitiveUTF8(materialize(h), n, materialize(3)), positionCaseInsensitiveUTF8(materialize(h), materialize(n), materialize(3)), positionCaseInsensitiveUTF8(h, materialize(n), materialize(3));

SELECT 'count';
WITH 'ẞẞẞẞ' AS h, 'ßß' AS n SELECT 'sharp s pairs', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));
WITH 'ⱥⱥ' AS h, 'Ⱥ' AS n SELECT 'a-bar pair', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));
WITH 'ⱥⱥⱥ' AS h, 'Ⱥⱥ' AS n SELECT 'a-bar triple, pair needle', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));
WITH concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)) AS h, 'kk' AS n SELECT 'kelvin four, kk', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));
WITH concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)) AS h, concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)) AS n SELECT 'kelvin four, kelvin pair', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));
WITH 'aaaa' AS h, 'aa' AS n SELECT 'ascii', countSubstringsCaseInsensitiveUTF8(h, n), countSubstringsCaseInsensitiveUTF8(materialize(h), n), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n)), countSubstringsCaseInsensitiveUTF8(h, materialize(n));

SELECT 'count with start_pos';
WITH 'ⱥⱥⱥⱥ' AS h, 'Ⱥ' AS n SELECT 'a-bar from 3', countSubstringsCaseInsensitiveUTF8(h, n, 3), countSubstringsCaseInsensitiveUTF8(materialize(h), n, 3), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n), 3), countSubstringsCaseInsensitiveUTF8(h, materialize(n), 3), countSubstringsCaseInsensitiveUTF8(materialize(h), n, materialize(3)), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n), materialize(3)), countSubstringsCaseInsensitiveUTF8(h, materialize(n), materialize(3));
WITH concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)) AS h, char(0xE2, 0x84, 0xAA) AS n SELECT 'kelvin from 2', countSubstringsCaseInsensitiveUTF8(h, n, 2), countSubstringsCaseInsensitiveUTF8(materialize(h), n, 2), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n), 2), countSubstringsCaseInsensitiveUTF8(h, materialize(n), 2), countSubstringsCaseInsensitiveUTF8(materialize(h), n, materialize(2)), countSubstringsCaseInsensitiveUTF8(materialize(h), materialize(n), materialize(2)), countSubstringsCaseInsensitiveUTF8(h, materialize(n), materialize(2));

SELECT 'ILIKE';
WITH 'ⱥ' AS h, concat('%', 'Ⱥ', '%') AS p SELECT 'A-bar to a-bar', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH 'STRAẞE' AS h, concat('%', 'straße', '%') AS p SELECT 'STRASSE', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz') AS h, concat('%', 'kzzz', '%') AS p SELECT 'kelvin vs k', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz') AS h, concat('%', concat(char(0xE2, 0x84, 0xAA), 'zzz'), '%') AS p SELECT 'kelvin to kelvin', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH concat('x', char(0xE2, 0x84, 0xAB)) AS h, concat('%', 'å', '%') AS p SELECT 'angstrom vs a-ring', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH char(0xE2, 0x84, 0xA6) AS h, concat('%', 'ω', '%') AS p SELECT 'ohm vs omega', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH char(0xCF, 0xB4) AS h, concat('%', 'θ', '%') AS p SELECT 'theta symbol vs theta', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH char(0xCE, 0x99) AS h, concat('%', char(0xCE, 0xB9), '%') AS p SELECT 'IOTA to iota', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH char(0xCE, 0xBC) AS h, concat('%', char(0xCE, 0x9C), '%') AS p SELECT 'mu to MU', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);
WITH char(0xC2, 0xB5) AS h, concat('%', char(0xCE, 0xBC), '%') AS p SELECT 'micro vs mu', h ILIKE p, materialize(h) ILIKE p, materialize(h) ILIKE materialize(p), h ILIKE materialize(p);

-- Tail length: a match must not depend on the bytes after it, also past the SIMD window.
-- Per group: const/const, column/const, column/column, const/column; every element must be equal.
SELECT 'tail, K in haystack, needle ak, all 1';
SELECT [positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 0)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 15)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 16)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 31)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 32)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 64)), 'ak'), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 100)), 'ak')];
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', n)), 'ak'), [0,15,16,31,32,64,100]);
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', n)), materialize('ak')), [0,15,16,31,32,64,100]);
SELECT [positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 0)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 15)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 16)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 31)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 32)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 64)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', 'K', repeat('x', 100)), materialize('ak'))];
SELECT 'tail, greek MU in haystack, needle mu, all 1';
SELECT [positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 0)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 15)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 16)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 31)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 32)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 64)), char(0xCE, 0xBC)), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 100)), char(0xCE, 0xBC))];
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', n)), char(0xCE, 0xBC)), [0,15,16,31,32,64,100]);
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', n)), materialize(char(0xCE, 0xBC))), [0,15,16,31,32,64,100]);
SELECT [positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 0)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 15)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 16)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 31)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 32)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 64)), materialize(char(0xCE, 0xBC))), positionCaseInsensitiveUTF8(concat(char(0xCE, 0x9C), repeat('x', 100)), materialize(char(0xCE, 0xBC)))];
SELECT 'tail, kelvin to kelvin, all 1';
SELECT [positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 0)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 15)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 16)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 31)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 32)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 64)), concat('a', char(0xE2, 0x84, 0xAA))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 100)), concat('a', char(0xE2, 0x84, 0xAA)))];
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', n)), concat('a', char(0xE2, 0x84, 0xAA))), [0,15,16,31,32,64,100]);
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', n)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), [0,15,16,31,32,64,100]);
SELECT [positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 0)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 15)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 16)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 31)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 32)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 64)), materialize(concat('a', char(0xE2, 0x84, 0xAA)))), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 100)), materialize(concat('a', char(0xE2, 0x84, 0xAA))))];
SELECT 'tail, kelvin vs k, all 0';
SELECT [positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 0)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 15)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 16)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 31)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 32)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 64)), 'ak'), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 100)), 'ak')];
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', n)), 'ak'), [0,15,16,31,32,64,100]);
SELECT arrayMap(n -> positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', n)), materialize('ak')), [0,15,16,31,32,64,100]);
SELECT [positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 0)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 15)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 16)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 31)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 32)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 64)), materialize('ak')), positionCaseInsensitiveUTF8(concat('a', char(0xE2, 0x84, 0xAA), repeat('x', 100)), materialize('ak'))];

SELECT 'neighbouring rows';
SELECT arrayMap(x -> positionCaseInsensitiveUTF8(x, 'Ⱥb'), ['ⱥ', 'b']);
SELECT arrayMap(x -> positionCaseInsensitiveUTF8(x, 'straße'), ['STRAẞ', 'Eisen']);
SELECT arrayMap(x -> positionCaseInsensitiveUTF8(x, concat(char(0xE2, 0x84, 0xAA), 'zz')), [concat(char(0xE2, 0x84, 0xAA), 'zz'), concat('a', char(0xE2, 0x84, 0xAA)), 'zz']);
SELECT arrayMap(x -> positionCaseInsensitiveUTF8(x, 'kzz'), [concat(char(0xE2, 0x84, 0xAA), 'zz'), 'kzz']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'Ⱥb'), ['ⱥ', 'b']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'straße'), ['STRAẞ', 'Eisen']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, concat(char(0xE2, 0x84, 0xAA), 'zz')), [concat(char(0xE2, 0x84, 0xAA), 'zz'), concat('a', char(0xE2, 0x84, 0xAA)), 'zz']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'kzz'), [concat(char(0xE2, 0x84, 0xAA), 'zz'), 'kzz']);
SELECT arrayMap(x -> x ILIKE concat('%', 'Ⱥb', '%'), ['ⱥ', 'b']);
SELECT arrayMap(x -> x ILIKE concat('%', 'straße', '%'), ['STRAẞ', 'Eisen']);
SELECT arrayMap(x -> x ILIKE concat('%', concat(char(0xE2, 0x84, 0xAA), 'zz'), '%'), [concat(char(0xE2, 0x84, 0xAA), 'zz'), concat('a', char(0xE2, 0x84, 0xAA)), 'zz']);
SELECT arrayMap(x -> x ILIKE concat('%', 'kzz', '%'), [concat(char(0xE2, 0x84, 0xAA), 'zz'), 'kzz']);

SELECT 'span advances the count';
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'ßß'), ['ẞẞẞẞ', 'ẞ', 'ẞẞẞ', 'ẞß']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'Ⱥ'), ['ⱥⱥ', 'Ⱥ', 'ⱥb']);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA))), [concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)), char(0xE2, 0x84, 0xAA), concat(char(0xE2, 0x84, 0xAA), 'k', char(0xE2, 0x84, 0xAA))]);
SELECT arrayMap(x -> countSubstringsCaseInsensitiveUTF8(x, 'kk'), [concat(char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA), char(0xE2, 0x84, 0xAA)), concat(char(0xE2, 0x84, 0xAA), 'k', char(0xE2, 0x84, 0xAA)), 'kkkk']);

SELECT 'rows of one block';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES
    (1, 'ⱥ'),
    (2, 'b'),
    (3, 'STRAẞ'),
    (4, 'Eisen'),
    (5, 'ⱥb'),
    (6, 'ẞẞẞẞ'),
    (7, 'ⱥⱥ'),
    (8, 'STRAẞE'),
    (9, concat('a', char(0xE2, 0x84, 0xAA))),
    (10, 'zz'),
    (11, concat(char(0xE2, 0x84, 0xAA), 'zz')),
    (12, 'kzz'),
    (13, 'KZZ');
-- position, needles 'Ⱥb', 'straße', kelvin + 'zz', 'kzz'
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'Ⱥb'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'straße'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 1;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'Ⱥb'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'straße'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 3;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'Ⱥb'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'straße'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 65536;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'Ⱥb'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'straße'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, positionCaseInsensitiveUTF8(s, 'kzz')))))
FROM (SELECT id, s FROM tab ORDER BY cityHash64(id))
SETTINGS max_block_size = 3;
-- count, needles 'ßß', 'Ⱥ', kelvin + 'zz', 'kzz'
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'ßß'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'Ⱥ'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 1;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'ßß'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'Ⱥ'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 3;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'ßß'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'Ⱥ'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, concat(char(0xE2, 0x84, 0xAA), 'zz')))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, countSubstringsCaseInsensitiveUTF8(s, 'kzz')))))
FROM tab
SETTINGS max_block_size = 65536;
-- ILIKE, needles 'Ⱥb', 'straße', kelvin + 'zz', 'kzz'
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'Ⱥb', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'straße', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', concat(char(0xE2, 0x84, 0xAA), 'zz'), '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'kzz', '%')))))
FROM tab
SETTINGS max_block_size = 1;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'Ⱥb', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'straße', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', concat(char(0xE2, 0x84, 0xAA), 'zz'), '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'kzz', '%')))))
FROM tab
SETTINGS max_block_size = 3;
SELECT
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'Ⱥb', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'straße', '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', concat(char(0xE2, 0x84, 0xAA), 'zz'), '%'))))),
    arrayMap(x -> x.2, arraySort(groupArray((id, s ILIKE concat('%', 'kzz', '%')))))
FROM tab
SETTINGS max_block_size = 65536;
DROP TABLE tab;

SELECT 'differential, the first line must print 1 and every other line 0';
-- `canon` folds the alphabet by the rule (the signs stay as they are), so a case-sensitive search over it is the expected result.
DROP TABLE IF EXISTS symbols;
DROP TABLE IF EXISTS needles;
DROP TABLE IF EXISTS haystacks;
CREATE TABLE symbols (c String) ENGINE = Memory;
CREATE TABLE needles (s String, canon String MATERIALIZED lower(replaceAll(replaceAll(replaceAll(s, 'ẞ', 'ß'), 'Ⱥ', 'ⱥ'), char(0xCE, 0x9C), char(0xCE, 0xBC)))) ENGINE = Memory;
CREATE TABLE haystacks (s String, canon String MATERIALIZED lower(replaceAll(replaceAll(replaceAll(s, 'ẞ', 'ß'), 'Ⱥ', 'ⱥ'), char(0xCE, 0x9C), char(0xCE, 0xBC)))) ENGINE = Memory;
INSERT INTO symbols SELECT arrayJoin(['a', 'b', 'k', char(0xE2, 0x84, 0xAA), 'ß', 'ẞ', 'ⱥ', 'Ⱥ', 's', 'S', char(0xC2, 0xB5), char(0xCE, 0xBC), char(0xCE, 0x9C)]);
INSERT INTO needles SELECT a.c FROM symbols AS a UNION ALL SELECT concat(a.c, b.c) FROM symbols AS a CROSS JOIN symbols AS b;
INSERT INTO haystacks SELECT '' UNION ALL SELECT s FROM needles UNION ALL SELECT concat(s, c) FROM needles CROSS JOIN symbols WHERE lengthUTF8(s) = 2;

SELECT count() > 0 FROM haystacks AS h CROSS JOIN needles AS n WHERE positionCaseInsensitiveUTF8(h.s, n.s) > 0;
SELECT count() FROM haystacks AS h CROSS JOIN needles AS n WHERE positionCaseInsensitiveUTF8(h.s, n.s) != positionUTF8(h.canon, n.canon);
SELECT count() FROM haystacks AS h CROSS JOIN needles AS n WHERE countSubstringsCaseInsensitiveUTF8(h.s, n.s) != countSubstrings(h.canon, n.canon);
SELECT count() FROM haystacks AS h CROSS JOIN needles AS n WHERE (h.s ILIKE concat('%', n.s, '%')) != (h.canon LIKE concat('%', n.canon, '%'));

-- constness: the constant-needle result equals the column-needle result, and the same for the haystack
SELECT
    sum(positionCaseInsensitiveUTF8(s, 'a') != positionCaseInsensitiveUTF8(s, materialize('a'))) +
    sum(positionCaseInsensitiveUTF8(s, char(0xE2, 0x84, 0xAA)) != positionCaseInsensitiveUTF8(s, materialize(char(0xE2, 0x84, 0xAA)))) +
    sum(positionCaseInsensitiveUTF8(s, 'ẞ') != positionCaseInsensitiveUTF8(s, materialize('ẞ'))) +
    sum(positionCaseInsensitiveUTF8(s, 'ⱥȺ') != positionCaseInsensitiveUTF8(s, materialize('ⱥȺ'))) +
    sum(positionCaseInsensitiveUTF8(s, concat('k', char(0xE2, 0x84, 0xAA))) != positionCaseInsensitiveUTF8(s, materialize(concat('k', char(0xE2, 0x84, 0xAA))))) +
    sum(positionCaseInsensitiveUTF8(s, char(0xCE, 0xBC)) != positionCaseInsensitiveUTF8(s, materialize(char(0xCE, 0xBC))))
FROM haystacks;
SELECT
    sum(countSubstringsCaseInsensitiveUTF8(s, 'a') != countSubstringsCaseInsensitiveUTF8(s, materialize('a'))) +
    sum(countSubstringsCaseInsensitiveUTF8(s, char(0xE2, 0x84, 0xAA)) != countSubstringsCaseInsensitiveUTF8(s, materialize(char(0xE2, 0x84, 0xAA)))) +
    sum(countSubstringsCaseInsensitiveUTF8(s, 'ẞ') != countSubstringsCaseInsensitiveUTF8(s, materialize('ẞ'))) +
    sum(countSubstringsCaseInsensitiveUTF8(s, 'ⱥȺ') != countSubstringsCaseInsensitiveUTF8(s, materialize('ⱥȺ'))) +
    sum(countSubstringsCaseInsensitiveUTF8(s, concat('k', char(0xE2, 0x84, 0xAA))) != countSubstringsCaseInsensitiveUTF8(s, materialize(concat('k', char(0xE2, 0x84, 0xAA))))) +
    sum(countSubstringsCaseInsensitiveUTF8(s, char(0xCE, 0xBC)) != countSubstringsCaseInsensitiveUTF8(s, materialize(char(0xCE, 0xBC))))
FROM haystacks;
SELECT
    sum((s ILIKE concat('%', 'a', '%')) != (s ILIKE materialize(concat('%', 'a', '%')))) +
    sum((s ILIKE concat('%', char(0xE2, 0x84, 0xAA), '%')) != (s ILIKE materialize(concat('%', char(0xE2, 0x84, 0xAA), '%')))) +
    sum((s ILIKE concat('%', 'ẞ', '%')) != (s ILIKE materialize(concat('%', 'ẞ', '%')))) +
    sum((s ILIKE concat('%', 'ⱥȺ', '%')) != (s ILIKE materialize(concat('%', 'ⱥȺ', '%')))) +
    sum((s ILIKE concat('%', concat('k', char(0xE2, 0x84, 0xAA)), '%')) != (s ILIKE materialize(concat('%', concat('k', char(0xE2, 0x84, 0xAA)), '%')))) +
    sum((s ILIKE concat('%', char(0xCE, 0xBC), '%')) != (s ILIKE materialize(concat('%', char(0xCE, 0xBC), '%'))))
FROM haystacks;
SELECT
    sum(positionCaseInsensitiveUTF8('a', s) != positionCaseInsensitiveUTF8(materialize('a'), s)) +
    sum(positionCaseInsensitiveUTF8(char(0xE2, 0x84, 0xAA), s) != positionCaseInsensitiveUTF8(materialize(char(0xE2, 0x84, 0xAA)), s)) +
    sum(positionCaseInsensitiveUTF8('ẞ', s) != positionCaseInsensitiveUTF8(materialize('ẞ'), s)) +
    sum(positionCaseInsensitiveUTF8('ⱥȺ', s) != positionCaseInsensitiveUTF8(materialize('ⱥȺ'), s)) +
    sum(positionCaseInsensitiveUTF8(concat('k', char(0xE2, 0x84, 0xAA)), s) != positionCaseInsensitiveUTF8(materialize(concat('k', char(0xE2, 0x84, 0xAA))), s)) +
    sum(positionCaseInsensitiveUTF8(char(0xCE, 0xBC), s) != positionCaseInsensitiveUTF8(materialize(char(0xCE, 0xBC)), s))
FROM needles;
SELECT
    sum(countSubstringsCaseInsensitiveUTF8('a', s) != countSubstringsCaseInsensitiveUTF8(materialize('a'), s)) +
    sum(countSubstringsCaseInsensitiveUTF8(char(0xE2, 0x84, 0xAA), s) != countSubstringsCaseInsensitiveUTF8(materialize(char(0xE2, 0x84, 0xAA)), s)) +
    sum(countSubstringsCaseInsensitiveUTF8('ẞ', s) != countSubstringsCaseInsensitiveUTF8(materialize('ẞ'), s)) +
    sum(countSubstringsCaseInsensitiveUTF8('ⱥȺ', s) != countSubstringsCaseInsensitiveUTF8(materialize('ⱥȺ'), s)) +
    sum(countSubstringsCaseInsensitiveUTF8(concat('k', char(0xE2, 0x84, 0xAA)), s) != countSubstringsCaseInsensitiveUTF8(materialize(concat('k', char(0xE2, 0x84, 0xAA))), s)) +
    sum(countSubstringsCaseInsensitiveUTF8(char(0xCE, 0xBC), s) != countSubstringsCaseInsensitiveUTF8(materialize(char(0xCE, 0xBC)), s))
FROM needles;
SELECT
    sum(('a' ILIKE concat('%', s, '%')) != (materialize('a') ILIKE concat('%', s, '%'))) +
    sum((char(0xE2, 0x84, 0xAA) ILIKE concat('%', s, '%')) != (materialize(char(0xE2, 0x84, 0xAA)) ILIKE concat('%', s, '%'))) +
    sum(('ẞ' ILIKE concat('%', s, '%')) != (materialize('ẞ') ILIKE concat('%', s, '%'))) +
    sum(('ⱥȺ' ILIKE concat('%', s, '%')) != (materialize('ⱥȺ') ILIKE concat('%', s, '%'))) +
    sum((concat('k', char(0xE2, 0x84, 0xAA)) ILIKE concat('%', s, '%')) != (materialize(concat('k', char(0xE2, 0x84, 0xAA))) ILIKE concat('%', s, '%'))) +
    sum((char(0xCE, 0xBC) ILIKE concat('%', s, '%')) != (materialize(char(0xCE, 0xBC)) ILIKE concat('%', s, '%')))
FROM needles;

DROP TABLE symbols;
DROP TABLE needles;
DROP TABLE haystacks;
