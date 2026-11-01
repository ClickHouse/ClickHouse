-- The embedded documentation renders examples as fenced code blocks. An example with an empty
-- query or response (e.g. some internal functions) must not emit an empty `Query`/`Response` code
-- block, which would otherwise render as an empty code box on a help surface like the documentation
-- web UI. Likewise, an example whose query/result is itself a fenced code block must not be wrapped
-- again into a (broken, empty) outer block.

-- No description contains an empty `Query` or `Response` fenced code block (the info line directly
-- followed by the closing fence, possibly across blank lines).
SELECT count() FROM system.documentation WHERE match(description, 'title=Query\n+```');
SELECT count() FROM system.documentation WHERE match(description, 'title=Response\n+```');
