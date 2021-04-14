SELECT repeat('abcdefghijklmnopqrstuvwxyz', number * 100) AS haystack, extractAllGroupsHorizontal(haystack, '(\\w)') AS matches FROM numbers(1023); -- { serverError 128 }
SELECT count(extractAllGroupsHorizontal(materialize('a'), '(a)')) FROM numbers(1000000) FORMAT Null; -- shouldn't fail
