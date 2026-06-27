-- Every documentation entry in `system.documentation` is rendered as Markdown.
-- A non-blank line indented by four or more spaces (or a tab) that is preceded by a
-- blank line is rendered by Markdown as an indented code block. When documentation is
-- authored indented inside an `R"(...)"` literal in the C++ sources, such a block renders
-- as code by accident, so admonitions like `:::note`, lists, and other markup leak out
-- verbatim.
--
-- This test guards against reintroducing that indentation. It looks for the actual
-- code-block trigger rather than "every line is indented": a non-blank line indented by
-- four spaces or a tab whose preceding line is blank. Note that for settings,
-- `renderSettingDoc` appends unindented `**Type:**`/`**Default:**` lines, so a check that
-- requires *every* line to be indented would never fire for them; this check still does.
-- The expected result is empty.

WITH splitByChar('\n', description) AS lines
SELECT name, type
FROM system.documentation
WHERE arrayExists(
        i -> match(lines[i], '^( {4}|\t)') AND match(lines[i], '\S')   -- indented, non-blank
             AND NOT match(lines[i - 1], '\S'),                         -- preceded by a blank line
        range(2, length(lines) + 1))
ORDER BY type, name;
