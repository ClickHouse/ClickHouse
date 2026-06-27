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
    arrayMap(l -> toUInt32(length(extract(l, '^( *(?:[-*+]|[0-9]+[.)])\s+)'))), lines) AS list_offset
SELECT name, type
FROM system.documentation
WHERE arrayExists(i ->
        nonblank[i]
        AND fences_through[i - 1] % 2 = 0                 -- line i is outside a fenced code block
        AND indent[i] >= 4
        AND NOT nonblank[i - 1]                            -- preceded by a blank line
        AND (
            -- top level: any indent of four or more columns is a code block
            empty(arrayFilter(j -> nonblank[j] AND indent[j] < indent[i], range(1, i)))
            -- otherwise: a code block only if indented four columns past the enclosing list content
            OR indent[i] >= (
                if(is_list[arrayMax(arrayFilter(j -> nonblank[j] AND indent[j] < indent[i], range(1, i)))],
                   list_offset[arrayMax(arrayFilter(j -> nonblank[j] AND indent[j] < indent[i], range(1, i)))],
                   indent[arrayMax(arrayFilter(j -> nonblank[j] AND indent[j] < indent[i], range(1, i)))]) + 4)
        ),
        range(2, n + 1))
ORDER BY type, name;
