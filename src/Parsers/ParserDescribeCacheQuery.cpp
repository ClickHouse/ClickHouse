#include <Parsers/ParserDescribeCacheQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{


bool ParserDescribeCacheQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword p_describe(Keyword::DESCRIBE);
    ParserKeyword p_desc(Keyword::DESC);
    ParserKeyword p_cache(Keyword::FILESYSTEM_CACHE);
    ParserLiteral p_cache_name;

    if ((!p_describe.ignore(pos, expected) && !p_desc.ignore(pos, expected))
        || !p_cache.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDescribeCacheQuery>();

    ASTPtr ast;
    if (!p_cache_name.parse(pos, ast, expected))
        return false;

    query->cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
    node = query;

    return true;
}

std::map<String, Documentation> ParserDescribeCacheQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["DESCRIBE FILESYSTEM CACHE"] =
    {
        .description = R"DOCS_MD(
Introduced in: v22.10.0

Returns one row with the settings of a [filesystem cache](/concepts/features/configuration/server-config/storing-data#using-local-cache) and its current state: `is_initialized`, `current_size` and `current_elements_num`. The names of the configured caches are returned by [`SHOW FILESYSTEM CACHES`](/reference/statements/show#show-filesystem-caches).

## Syntax {#syntax}

```sql
DESC|DESCRIBE FILESYSTEM CACHE 'cache_name'
```

A name that no cache has causes `BAD_ARGUMENTS`.

## Example {#example}

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 'local_cache' FORMAT Vertical
```

```text title="Response"
Row 1:
──────
cache_name:                                  local_cache
path:                                        /var/lib/clickhouse/caches/local_cache/
max_size:                                    22548578304 -- 22.55 billion
max_elements:                                10000000 -- 10.00 million
max_file_segment_size:                       33554432 -- 33.55 million
boundary_alignment:                          4194304 -- 4.19 million
reserve_granularity:                         4194304 -- 4.19 million
cache_on_write_operations:                   1
cache_policy:                                lru
slru_size_ratio:                             0.6
background_download_threads:                 5
background_download_queue_size_limit:        5000
background_download_max_file_segment_size:   4194304 -- 4.19 million
load_metadata_threads:                       16
load_metadata_asynchronously:                0
keep_free_space_size_ratio:                  0
keep_free_space_elements_ratio:              0
keep_free_space_remove_batch:                250
keep_free_space_eviction_threads:            1
invalidated_entries_cleanup_interval_ms:     10000
invalidated_entries_cleanup_threshold:       1000
invalidated_entries_cleanup_remove_batch:    250
enable_filesystem_query_cache_limit:         0
cache_hits_threshold:                        0
enable_bypass_cache_with_threshold:          0
bypass_cache_threshold:                      268435456 -- 268.44 million
write_cache_per_user_id_directory:           0
allow_dynamic_cache_resize:                  0
dynamic_resize_lock_wait_ms:                 1000
max_size_ratio_to_total_space:               0
skip_cache_on_disk_failure:                  0
use_split_cache:                             0
split_cache_ratio:                           0.1
system_cache_extensions:                     .txt,.json,.idx,.cidx,.dat
overcommit_eviction_evict_step:              10485760 -- 10.49 million
check_cache_probability:                     0.001
idle_client_ttl_sec:                         604800
idle_client_check_interval_sec:              0
client_weight_persist_interval_sec:          300
idle_client_eviction_threads:                4
expose_prometheus_eviction_metrics:          0
expose_prometheus_eviction_metrics_per_user: 0
use_real_disk_size:                          0
drop_cache_threads:                          16
is_initialized:                              1
current_size:                                0
current_elements_num:                        0
```

## Related content {#related-content}

- [`system.filesystem_cache`](/reference/system-tables/filesystem_cache)
- [Using local cache](/concepts/features/configuration/server-config/storing-data#using-local-cache)
)DOCS_MD",
        .syntax = R"(
DESC|DESCRIBE FILESYSTEM CACHE 'cache_name'
)",
        .related = {"SHOW", "SYSTEM", "DESCRIBE TABLE"},
    };

    return documentation;
}


}
