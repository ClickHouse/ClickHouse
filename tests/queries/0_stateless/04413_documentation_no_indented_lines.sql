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

WITH
    arrayMap(l -> replaceAll(l, '\t', '    '), splitByChar('\n', description)) AS lines,
    length(lines) AS n,
    arrayMap(l -> toUInt32(length(l) - length(replaceRegexpOne(l, '^ +', ''))), lines) AS indent,
    arrayMap(l -> match(l, '\S'), lines) AS nonblank,
    arrayCumSum(arrayMap(l -> toUInt8(match(l, '^ *```')), lines)) AS fences_through,
    arrayMap(l -> match(l, '^ *([-*+]|[0-9]+[.)])\s'), lines) AS is_list,
    arrayMap(l -> toUInt32(length(extract(l, '^( *(?:[-*+]|[0-9]+[.)])\s+)'))), lines) AS list_offset,
    arrayMap((is_l, lo, ind) -> if(is_l, lo, ind) + 4, is_list, list_offset, indent) AS container_indent,
    arrayFilter(
        (i, nb, prev_nb, prev_fences, ind) ->
            nb
            AND prev_fences % 2 = 0                        -- line i is outside a fenced code block
            AND ind >= 4
            AND NOT prev_nb,                               -- preceded by a blank line
        range(2, n + 1),
        arraySlice(nonblank, 2),
        arraySlice(nonblank, 1, n - 1),
        arraySlice(fences_through, 1, n - 1),
        arraySlice(indent, 2)
    ) AS candidates
SELECT name, type
FROM system.documentation
WHERE arrayExists(i ->
        -- At the top level `arrayLast` returns 0 (no prior non-blank line with smaller indent),
        -- which every candidate (`indent[i] >= 4`) satisfies; inside a container it returns the
        -- nearest enclosing line's content offset + 4.
        indent[i] >= arrayLast(
            (ci, nb, ind) -> nb AND ind < indent[i],
            arraySlice(container_indent, 1, i - 1),
            arraySlice(nonblank, 1, i - 1),
            arraySlice(indent, 1, i - 1)),
        candidates)
ORDER BY type, name;
