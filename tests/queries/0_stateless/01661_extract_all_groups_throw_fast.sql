SELECT repeat('abcdefghijklmnopqrstuvwxyz', number * 100) AS haystack, extractAllGroupsHorizontal(haystack, '(\\w)') AS matches FROM numbers(1023); -- { serverError 128 }
