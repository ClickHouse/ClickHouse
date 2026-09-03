-- The JIT-compiled matcher used to compile `.`, negated classes and `\D`/`\W`/`\S` into byte sets, while
-- RE2 runs in UTF-8 mode and matches whole code points: on a haystack that is not valid UTF-8 the two
-- disagreed, so the result of a query changed once the pattern had been used often enough to be compiled.

-- `min_count_to_compile_regular_expression = 0` compiles on the first use, so both engines are exercised
-- by the pair of queries below rather than by the number of rows.
SET min_count_to_compile_regular_expression = 0;

SELECT
    match(materialize(char(255)), '.+'),
    countMatches(materialize(char(255)), '.+'),
    extract(materialize(char(255)), '.+') = '',
    match(materialize(char(255)), '[^a]'),
    match(materialize(char(255)), '\\D');

SET min_count_to_compile_regular_expression = 100000;

SELECT
    match(materialize(char(255)), '.+'),
    countMatches(materialize(char(255)), '.+'),
    extract(materialize(char(255)), '.+') = '',
    match(materialize(char(255)), '[^a]'),
    match(materialize(char(255)), '\\D');

-- Valid UTF-8 haystacks and ASCII-only patterns are unaffected, compiled or not.
SET min_count_to_compile_regular_expression = 0;
SELECT match(materialize('a1'), '^[a-z][0-9]$'), match(materialize('ab'), '^[a-z][0-9]$'), match(materialize('héllo'), '.+'), extract(materialize('héllo'), 'h(.)llo');
SET min_count_to_compile_regular_expression = 100000;
SELECT match(materialize('a1'), '^[a-z][0-9]$'), match(materialize('ab'), '^[a-z][0-9]$'), match(materialize('héllo'), '.+'), extract(materialize('héllo'), 'h(.)llo');
