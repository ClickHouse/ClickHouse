-- Every documentation entry in `system.documentation` is rendered as Markdown by the built-in
-- `/docs` Web UI. Markdown turns a line indented by four or more columns into an indented CODE
-- block, so documentation that was authored indented inside an `R"(...)"` literal in the C++
-- sources renders as code by accident: admonitions (`:::note`), lists, and prose leak out verbatim.
--
-- This test guards against reintroducing that bug. It reproduces the relevant CommonMark rule (as
-- implemented by the `marked` renderer the page uses) closely enough to flag the real bugs without
-- false positives:
--   * a code block is a non-blank line preceded by a blank line and indented by four columns or
--     more *relative to its container*;
--   * the container indent is four columns deeper than the content offset of the nearest enclosing
--     list item (so a sub-bullet or a wrapped list paragraph is NOT a code block), or zero at the
--     top level;
--   * lines inside a fenced code block (between ``` fences) are skipped — their indentation is the
--     intended content of the fence.
-- Tabs are expanded to four spaces first. The expected result is empty.
--
-- The query first finds the candidates: non-blank lines outside a fenced code block, indented by four
-- columns or more and preceded by a blank line (the previous line is read from the arrays shifted by one).
-- For each candidate, it then looks up its container, the nearest preceding non-blank line indented less,
-- and checks whether the candidate is indented four columns past the content of the container.
-- For an entry with n lines and k candidates, this is O(n + k * n). `system.documentation` has thousands
-- of entries with about 180000 lines in total, up to about 7000 in one entry (`system.events`), but only
-- a few dozen candidates.

WITH
    arrayMap(l -> replaceAll(l, '\t', '    '), splitByChar('\n', description)) AS lines,
    arrayMap(l -> toUInt32(length(l) - length(replaceRegexpOne(l, '^ +', ''))), lines) AS indent,
    arrayMap(l -> match(l, '\S'), lines) AS nonblank,
    arrayCumSum(arrayMap(l -> toUInt8(match(l, '^ *```')), lines)) AS fences_through,
    arrayMap(l -> match(l, '^ *([-*+]|[0-9]+[.)])\s'), lines) AS is_list,
    arrayMap(l -> toUInt32(length(extract(l, '^( *(?:[-*+]|[0-9]+[.)])\s+)'))), lines) AS list_offset,
    -- The lines that start a code block if they are indented far enough relative to their container.
    arrayFilter((i, line_nonblank, prev_nonblank, line_indent, prev_fences_through) ->
            line_nonblank
            AND NOT prev_nonblank                          -- preceded by a blank line
            AND line_indent >= 4
            AND prev_fences_through % 2 = 0,               -- outside a fenced code block
        arrayEnumerate(lines),
        nonblank,
        arrayPushFront(arrayPopBack(nonblank), 1),         -- the first line has no blank line before it
        indent,
        arrayPushFront(arrayPopBack(fences_through), 0)) AS candidates,
    -- The container of each candidate: the nearest preceding non-blank line indented less, 0 at the top level.
    arrayMap(i -> arrayLastIndex((line_nonblank, line_indent) -> line_nonblank AND line_indent < indent[i],
            arraySlice(nonblank, 1, i - 1), arraySlice(indent, 1, i - 1)),
        candidates) AS containers
SELECT name, type
FROM system.documentation
WHERE arrayExists((i, container) ->
        -- top level: any indent of four or more columns is a code block
        container = 0
        -- otherwise: a code block only if indented four columns past the enclosing list content
        OR indent[i] >= if(is_list[container], list_offset[container], indent[container]) + 4,
        candidates, containers)
ORDER BY type, name;
