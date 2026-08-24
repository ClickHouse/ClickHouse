-- Regression for UB (stack-use-after-scope) in extactAll().
-- The `values` pattern `:"\0[^"]*)"` is a malformed regexp (unbalanced parenthesis). It used to be
-- silently accepted because the analyzer stopped at the NUL byte and treated the pattern as the
-- trivial literal `:"`, never compiling re2. The NUL is now an ordinary literal byte, so re2
-- compiles the full pattern and correctly rejects it; the analyzer still runs first, which keeps
-- the original use-after-scope regression coverage.
SELECT
    '{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}' AS json,
    extractAll(json, '"([^"]*)":') AS keys,
    extractAll(json, ':"\0[^"]*)"') AS values; -- { serverError CANNOT_COMPILE_REGEXP }
