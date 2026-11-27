-- Data-type documentation generator.
-- Usage: one page per type family, driven by the doc page's basename, e.g.:
--   clickhouse --param_type='array' --queries-file generate-data-types.sql
--
-- system.data_type_families.description holds the full Markdown body of the
-- type's reference page (populated from the `Documentation` attached at
-- registerDataType time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. The page basename is matched to the family name case- and
-- separator-insensitively; aliases (case-insensitive family names) carry no
-- description, so notEmpty()+LIMIT 1 selects the canonical family. Pages that
-- are not a single family (index, binary-encoding) produce no output and are
-- skipped.
SELECT description
FROM system.data_type_families
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({type:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-data-type.md' TRUNCATE FORMAT LineAsString
