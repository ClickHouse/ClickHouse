-- Format documentation generator.
-- Usage: one page per format, driven by the doc page's basename, e.g.:
--   clickhouse --param_format='JSONEachRow' --queries-file generate-formats.sql
--
-- system.formats.description holds the full Markdown body of the format's
-- reference page (populated from the `Documentation` attached via
-- FormatFactory::setDocumentation). We emit that body verbatim; the Python
-- port then applies the same Docusaurus->Mintlify body transforms it runs on
-- migrated pages. The page basename is matched to the format name case- and
-- separator-insensitively; format aliases carry only a short "Alias of ..."
-- description, so notEmpty()+LIMIT 1 selects the canonical format. Pages that
-- are not a single format (index) produce no output and are skipped.
SELECT description
FROM system.formats
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({format:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-format.md' TRUNCATE FORMAT LineAsString
