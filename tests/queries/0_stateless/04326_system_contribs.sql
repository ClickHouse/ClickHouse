-- Schema is stable.
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'contribs' ORDER BY position;

-- Several well-known contribs must be present.
SELECT name FROM system.contribs WHERE name IN ('boost', 'zlib-ng', 'NuRaft', 'cctz', 'zstd') ORDER BY name;

-- Every row has the expected shape.
SELECT count() > 0 FROM system.contribs;
SELECT count() FROM system.contribs WHERE name = '' OR path = '' OR commit = '' OR submodule_url = '';
SELECT count() FROM system.contribs WHERE length(commit) != 40 OR lower(commit) != commit;
SELECT count() FROM system.contribs WHERE NOT startsWith(path, '/contrib/');

-- rust_vendor is excluded for v1.
SELECT count() FROM system.contribs WHERE path LIKE '/contrib/rust_vendor/%';

-- ClickHouse-owned submodule URLs must be flagged as forks.
SELECT count() FROM system.contribs WHERE submodule_url LIKE 'https://github.com/ClickHouse/%' AND is_fork = 0;
