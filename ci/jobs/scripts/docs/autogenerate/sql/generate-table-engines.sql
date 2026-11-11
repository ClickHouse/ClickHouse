-- Table-engine documentation generator.
-- Usage: one page per engine, driven by the doc page's basename, e.g.:
--   clickhouse --param_engine='mergetree' --queries-file generate-table-engines.sql
--
-- system.table_engines.description holds the full Markdown body of the engine's
-- reference page (populated from the `Documentation` attached at registerStorage
-- time). We emit that body verbatim; the Python port then applies the same
-- Docusaurus->Mintlify body transforms it runs on migrated pages.
--
-- The page basename (`mergetree`, `embedded-rocksdb`, `time-series`, ...) is
-- matched to the engine name (`MergeTree`, `EmbeddedRocksDB`, `TimeSeries`, ...)
-- case- and separator-insensitively, so pages that are not engines (concept
-- pages such as `replication` or `annindexes`) produce no output and are skipped.
SELECT description
FROM system.table_engines
WHERE lower(replaceRegexpAll(name, '[^0-9a-zA-Z]', ''))
    = lower(replaceRegexpAll({engine:String}, '[^0-9a-zA-Z]', ''))
  AND notEmpty(description)
ORDER BY name ASC
LIMIT 1
INTO OUTFILE 'temp-table-engine.md' TRUNCATE FORMAT LineAsString
