-- Window-function documentation generator.
-- Usage: one page per window function, driven by the doc page's basename, e.g.:
--   clickhouse --param_fn='row_number' --queries-file generate-window-functions.sql
--
-- Window functions are registered in the aggregate-function factory and exposed
-- in system.functions; their description holds the full Markdown body of the
-- reference page (populated from the FunctionDocumentation attached at
-- registerFunction time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. The page basename is matched to the function name case- and
-- separator-insensitively (so dense_rank matches denseRank). Pages whose
-- function carries no description produce no output and are skipped.
SELECT description
FROM system.functions
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({fn:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-window-function.md' TRUNCATE FORMAT LineAsString
