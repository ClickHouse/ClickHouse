-- Dictionary-source documentation generator.
-- Usage: one page per source, driven by the doc page's basename, e.g.:
--   clickhouse --param_source='file' --queries-file generate-dictionary-sources.sql
--
-- `system.dictionary_sources.description` holds the full Markdown body of the
-- source's reference page (populated from the `Documentation` attached at
-- `registerSource` time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. Pages that do not correspond to a source (such as overview) produce
-- no output and are skipped.
SELECT description
FROM system.dictionary_sources
WHERE name = {source:String}
  AND notEmpty(description)
LIMIT 1
INTO OUTFILE 'temp-dictionary-source.md' TRUNCATE FORMAT LineAsString
