-- Database-engine documentation generator.
-- Usage: one page per engine, driven by the doc page's basename, e.g.:
--   clickhouse --param_engine='replicated' --queries-file generate-database-engines.sql
--
-- system.database_engines.description holds the full Markdown body of the
-- engine's reference page (populated from the `Documentation` attached at
-- registerDatabase time). We emit that body verbatim; the Python port then
-- applies the same Docusaurus->Mintlify body transforms it runs on migrated
-- pages. The page basename is matched to the engine name case- and
-- separator-insensitively, so non-engine pages (e.g. index) produce no output
-- and are skipped.
SELECT description
FROM system.database_engines
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({engine:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-database-engine.md' TRUNCATE FORMAT LineAsString
