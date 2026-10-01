-- A full ATTACH that copies a source table must keep its storage clause before AS.
SELECT position(formatQuerySingleLine('ATTACH TABLE dst UUID ''01234567-89ab-cdef-0123-456789abcdef'' ENGINE = MergeTree ORDER BY a AS src'), 'ENGINE = MergeTree ORDER BY a AS src') > 0;
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE dst UUID ''01234567-89ab-cdef-0123-456789abcdef'' ENGINE = MergeTree ORDER BY a AS src')) = formatQuerySingleLine('ATTACH TABLE dst UUID ''01234567-89ab-cdef-0123-456789abcdef'' ENGINE = MergeTree ORDER BY a AS src');
SELECT position(formatQuerySingleLine('ATTACH TABLE dst UUID ''01234567-89ab-cdef-0123-456789abcdef'' ENGINE = MergeTree ORDER BY a SETTINGS max_projections = 1 AS src'), 'SETTINGS max_projections = 1 AS src') > 0;
