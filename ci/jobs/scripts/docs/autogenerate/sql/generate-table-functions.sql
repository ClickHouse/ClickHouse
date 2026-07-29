-- Table-function documentation generator.
-- Usage: one page per table function, driven by the doc page's basename, e.g.:
--   clickhouse --param_tf='s3' --queries-file generate-table-functions.sql
--
-- system.table_functions.description holds the full Markdown body of the table
-- function's reference page (populated from the FunctionDocumentation attached
-- at registerFunction time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. The page basename is matched to the function name case- and
-- separator-insensitively; functions with no description produce no output and
-- are skipped.
SELECT description
FROM system.table_functions
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({tf:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-table-function.md' TRUNCATE FORMAT LineAsString
