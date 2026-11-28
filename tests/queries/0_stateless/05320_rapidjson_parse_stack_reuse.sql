-- Tags: no-fasttest
-- no-fasttest: the rapidjson parser is not built in the fast test.

-- Documents whose parse stacks fit in the parser's inline buffer interleaved, in one block, with documents whose
-- stacks outgrow it: a long string, many members, deep nesting.
SET allow_simdjson = 0;

SELECT
    number,
    JSONExtractInt(doc, 'a'),
    length(JSONExtractString(doc, 's')),
    JSONExtractString(doc, 's') = repeat(toString(number), JSONExtractInt(doc, 'n')),
    JSONLength(doc)
FROM
(
    SELECT number, multiIf(
        number % 4 = 0, concat('{"a":1,"n":1,"s":"', toString(number), '"}'),
        number % 4 = 1, concat('{"a":2,"n":5000,"s":"', repeat(toString(number), 5000), '"}'),
        number % 4 = 2, concat('{"a":3,"n":1,"s":"', toString(number), '",', arrayStringConcat(arrayMap(i -> concat('"k', toString(i), '":', toString(i)), range(300)), ','), '}'),
        concat('{"a":4,"n":1,"s":"', toString(number), '","d":', repeat('{"d":', 100), '0', repeat('}', 101))) AS doc
    FROM numbers(8)
)
ORDER BY number;
