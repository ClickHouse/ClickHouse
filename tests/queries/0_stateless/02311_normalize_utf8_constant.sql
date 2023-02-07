-- Tags: no-fasttest

SELECT
    'â' AS s,
    normalizeUTF8NFC(s) s1,
    normalizeUTF8NFD(s) s2,
    normalizeUTF8NFKC(s) s3,
    normalizeUTF8NFKD(s) s4,
    hex(s),
    hex(s1),
    hex(s2),
    hex(s3),
    hex(s4);
