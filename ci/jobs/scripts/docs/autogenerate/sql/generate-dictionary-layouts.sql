-- Dictionary-layout documentation generator.
-- Usage: one page per layout family, driven by the doc page's basename, e.g.:
--   clickhouse --param_layout='flat' --queries-file generate-dictionary-layouts.sql
--
-- `system.dictionary_layouts.description` holds the full Markdown body of the
-- layout family's reference page (populated from the `Documentation` attached
-- at `registerLayout` time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. Pages that do not correspond to a layout (such as `overview` and
-- `hierarchical`) produce no output and are skipped.
SELECT description
FROM system.dictionary_layouts
WHERE name = {layout:String}
  AND notEmpty(description)
LIMIT 1
INTO OUTFILE 'temp-dictionary-layout.md' TRUNCATE FORMAT LineAsString
