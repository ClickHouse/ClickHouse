-- Every non-alias built-in function must carry a non-empty description in its structured documentation.
-- Aliases inherit their target's documentation, so they are exempt; internal functions carry the
-- shared internal-function documentation (FunctionDocumentation::INTERNAL_FUNCTION_DOCS).
-- Restrict to `origin = 'System'` so user-defined functions created by other tests running in parallel
-- against the shared server (which have no structured documentation) do not pollute the result.
SELECT name FROM system.functions WHERE length(description) < 10 AND alias_to = '' AND origin = 'System';
