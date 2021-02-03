WITH
    '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' AS x,
    replaceRegexpAll(x, '.', ' ') AS spaces,
    concat(substring(spaces, 1, rand(1) % 62), substring(x, 1, rand(2) % 62), substring(spaces, 1, rand(3) % 62)) AS s,
    trimLeft(s) AS sl,
    trimRight(s) AS sr,
    trimBoth(s) AS t,
    replaceRegexpOne(s, '^ +', '') AS slr,
    replaceRegexpOne(s, ' +$', '') AS srr,
    replaceRegexpOne(s, '^ *(.*?) *$', '\\1') AS tr
SELECT
    replaceAll(s, ' ', '_'),
    replaceAll(sl, ' ', '_'),
    replaceAll(slr, ' ', '_'),
    replaceAll(sr, ' ', '_'),
    replaceAll(srr, ' ', '_'),
    replaceAll(t, ' ', '_'),
    replaceAll(tr, ' ', '_')
FROM numbers(100000)
WHERE NOT ((sl = slr) AND (sr = srr) AND (t = tr))
