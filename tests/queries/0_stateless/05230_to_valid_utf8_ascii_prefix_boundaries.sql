-- Check valid and invalid sequences following ASCII prefixes on both sides of SIMD and library thresholds.
SELECT countIf(
    toValidUTF8(concat(repeat('a', prefix), sample.1, repeat('z', suffix)))
    != concat(repeat('a', prefix), sample.2, repeat('z', suffix)))
FROM
(
    SELECT
        arrayJoin([0, 1, 7, 8, 15, 16, 17, 31, 32, 47, 63, 64, 127, 128]) AS prefix,
        arrayJoin([0, 1, 15, 16, 17, 128]) AS suffix,
        arrayJoin([
            ('\xC2\xA3', '\xC2\xA3'),
            ('\xE2\x82\xAC', '\xE2\x82\xAC'),
            ('\xF0\x9F\x98\x80', '\xF0\x9F\x98\x80'),
            ('\xFF', '\xEF\xBF\xBD'),
            ('\xC0\xAF', '\xEF\xBF\xBD'),
            ('\xED\xA0\x80', '\xEF\xBF\xBD'),
            ('\xF0\x80\x80\x80', '\xEF\xBF\xBD'),
            ('\xF4\x90\x80\x80', '\xEF\xBF\xBD'),
            ('\xE2\x82X', '\xEF\xBF\xBDX'),
            ('\xF0\x9F\x98X', '\xEF\xBF\xBDX'),
            ('\xFF\xEF\xBF\xBD\xFF', '\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD')]) AS sample
);

-- Preserve the current whole-value replacement policy for a nominal sequence that cannot fit in the tail.
SELECT countIf(
    toValidUTF8(concat(repeat('a', prefix), tail))
    != concat(repeat('a', prefix), '\xEF\xBF\xBD'))
FROM
(
    SELECT
        arrayJoin([0, 1, 15, 16, 31, 32, 63, 64, 127, 128]) AS prefix,
        arrayJoin(['\xC2', '\xE2\x82', '\xF0\x9F\x98', '\xE2X', '\xF0XY']) AS tail
);
