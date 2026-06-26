-- Every documentation entry in `system.documentation` is rendered as Markdown.
-- If every line of a description is indented (e.g. because the text was written
-- indented inside an `R"(...)"` literal in the C++ sources), Markdown treats the
-- whole body as a code block, so admonitions like `:::note` and other markup leak
-- out verbatim. The renderer trims the first line, so the telltale sign of the bug
-- is that every non-blank line *after the first* is indented.
--
-- This test guards against reintroducing such indentation. The expected result is empty.

SELECT name, type
FROM system.documentation
WHERE
    -- there is at least one non-blank line after the first
    arrayExists(line -> match(line, '\\S'), arraySlice(splitByChar('\n', description), 2))
    -- and every non-blank line after the first is indented
    AND arrayAll(line -> NOT match(line, '\\S') OR match(line, '^\\s'),
                 arraySlice(splitByChar('\n', description), 2))
ORDER BY type, name;
