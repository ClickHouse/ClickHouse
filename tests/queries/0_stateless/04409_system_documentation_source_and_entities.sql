-- system.documentation exposes a `source` column with the repository-relative path of the source file where
-- each entity's documentation is defined, and documents additional kinds of entities: compression codecs,
-- profile events, current metrics, asynchronous metrics, and the system tables themselves.

-- The `source` column exists and is a String.
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'documentation' AND name = 'source';

-- The source paths are always relative to the repository root, never absolute (this also covers builds that do
-- not remap source paths, where the captured paths are absolute and must be normalized).
SELECT count() FROM system.documentation WHERE source LIKE '/%';

-- Every non-alias documented entity has a source (an alias whose canonical entity is not itself documented,
-- e.g. an alias of an internal function, may have an empty source).
SELECT count() FROM system.documentation WHERE source = '' AND description NOT LIKE 'Alias of %';

-- The additional kinds of entities are represented.
SELECT toString(type) AS t, count() > 0
FROM system.documentation
WHERE t IN ('Compression Codec', 'Profile Event', 'Current Metric', 'Asynchronous Metric', 'System Table')
GROUP BY t ORDER BY t;

-- A selection of well-known entities of each additional kind is present.
SELECT toString(type) AS t, name FROM system.documentation
WHERE (t, name) IN (
    ('Compression Codec', 'LZ4'),
    ('Profile Event', 'Query'),
    ('Current Metric', 'Query'),
    ('System Table', 'documentation'))
ORDER BY t, name;

-- The entities documented in a single source file each carry that file as their source.
SELECT DISTINCT source FROM system.documentation WHERE type = 'Setting';
SELECT DISTINCT source FROM system.documentation WHERE type = 'MergeTree Setting';
SELECT DISTINCT source FROM system.documentation WHERE type = 'Server Setting';
SELECT DISTINCT source FROM system.documentation WHERE type = 'Profile Event';
SELECT DISTINCT source FROM system.documentation WHERE type = 'Current Metric';

-- Asynchronous metrics are produced across several files, so each carries its own source; the source is never empty
-- and always lives under `src/`.
SELECT count() FROM system.documentation WHERE type = 'Asynchronous Metric' AND (source = '' OR source NOT LIKE 'src/%');

-- Each system table points to its own storage source file (relative to the repository root), so the source is
-- never empty and always lives under `src/Storages/`.
SELECT count() FROM system.documentation WHERE type = 'System Table' AND (source = '' OR source NOT LIKE 'src/Storages/%');
SELECT source FROM system.documentation WHERE type = 'System Table' AND name = 'events';

-- The source of a documentation object points to the source file that defines the component, relative to the
-- repository root: a function to its file, and a compression codec to its file.
SELECT source FROM system.documentation WHERE type = 'Function' AND name = 'moduloOrNull';
SELECT source FROM system.documentation WHERE type = 'Compression Codec' AND name = 'LZ4';

-- A system table documents itself with its table comment and the list of its columns.
SELECT description LIKE '%**Columns**%' FROM system.documentation WHERE type = 'System Table' AND name = 'documentation';

-- The documentation of a setting (of any kind) includes its type and default value.
SELECT description LIKE '%**Type:**%' AND description LIKE '%**Default:**%'
FROM system.documentation WHERE type = 'Setting' AND name = 'max_threads';
SELECT description LIKE '%**Type:**%' AND description LIKE '%**Default:**%'
FROM system.documentation WHERE type = 'MergeTree Setting' AND name = 'index_granularity';
SELECT description LIKE '%**Type:**%' AND description LIKE '%**Default:**%'
FROM system.documentation WHERE type = 'Server Setting' AND name = 'max_server_memory_usage';
