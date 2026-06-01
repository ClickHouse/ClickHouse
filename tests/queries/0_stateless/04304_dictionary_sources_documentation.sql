-- Dictionary sources expose embedded documentation via system.dictionary_sources.

-- Every registered source must have a non-empty description and syntax.
SELECT count() > 10 AS has_sources, countIf(length(description) = 0) AS undocumented, countIf(length(syntax) = 0) AS no_syntax
FROM system.dictionary_sources;

-- Representative sources have a description.
SELECT name, length(description) > 0 AS has_description
FROM system.dictionary_sources
WHERE name IN ('clickhouse', 'mysql', 'file', 'http', 'executable')
ORDER BY name;

-- Related sources are exposed as an array.
SELECT related
FROM system.dictionary_sources
WHERE name = 'clickhouse';

-- The ytsaurus syntax must use the real configuration key `http_proxy_urls`, not `http_proxy_url`.
SELECT name, position(syntax, 'http_proxy_urls') > 0 AS uses_correct_key
FROM system.dictionary_sources
WHERE name = 'ytsaurus';
