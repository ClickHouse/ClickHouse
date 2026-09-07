#include <Dictionaries/CacheDictionary.h>
#include <Dictionaries/CacheDictionaryStorage.h>
#include <Dictionaries/SSDCacheDictionaryStorage.h>
#include <Common/filesystemHelpers.h>
#include <Core/Settings.h>

#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
}

namespace ErrorCodes
{
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

static CacheDictionaryStorageConfiguration parseCacheStorageConfiguration(
    const Poco::Util::AbstractConfiguration & config,
    const String & full_name,
    const String & layout_type,
    const String & dictionary_layout_prefix,
    const DictionaryLifetime & dict_lifetime)
{
    size_t size = config.getUInt64(dictionary_layout_prefix + ".size_in_cells");
    if (size == 0)
        throw Exception(ErrorCodes::TOO_SMALL_BUFFER_SIZE,
                        "{}: dictionary of layout '{}' setting 'size_in_cells' must be greater than 0",
                        full_name, layout_type);

    size_t dict_lifetime_seconds = static_cast<size_t>(dict_lifetime.max_sec);
    size_t strict_max_lifetime_seconds = config.getUInt64(dictionary_layout_prefix + ".strict_max_lifetime_seconds", dict_lifetime_seconds);
    size_t rounded_size = roundUpToPowerOfTwoOrZero(size);

    CacheDictionaryStorageConfiguration storage_configuration
    {
        .max_size_in_cells = rounded_size,
        .strict_max_lifetime_seconds = strict_max_lifetime_seconds,
        .lifetime = dict_lifetime
    };

    return storage_configuration;
}

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_DARWIN)

static SSDCacheDictionaryStorageConfiguration parseSSDCacheStorageConfiguration(
    const Poco::Util::AbstractConfiguration & config,
    const String & full_name,
    const String & layout_type,
    const String & dictionary_layout_prefix,
    const DictionaryLifetime & dict_lifetime)
{
    size_t strict_max_lifetime_seconds = config.getUInt64(dictionary_layout_prefix + ".strict_max_lifetime_seconds", static_cast<size_t>(dict_lifetime.max_sec));

    static constexpr size_t DEFAULT_SSD_BLOCK_SIZE_BYTES = DEFAULT_AIO_FILE_BLOCK_SIZE;
    static constexpr size_t DEFAULT_FILE_SIZE_BYTES = 4 * 1024 * 1024 * 1024ULL;
    static constexpr size_t DEFAULT_READ_BUFFER_SIZE_BYTES = 16 * DEFAULT_SSD_BLOCK_SIZE_BYTES;
    static constexpr size_t DEFAULT_WRITE_BUFFER_SIZE_BYTES = DEFAULT_SSD_BLOCK_SIZE_BYTES;

    static constexpr size_t DEFAULT_PARTITIONS_COUNT = 16;

    size_t max_partitions_count = config.getInt64(dictionary_layout_prefix + ".max_partitions_count", DEFAULT_PARTITIONS_COUNT);

    size_t block_size = config.getInt64(dictionary_layout_prefix + ".block_size", DEFAULT_SSD_BLOCK_SIZE_BYTES);
    size_t file_size = config.getInt64(dictionary_layout_prefix + ".file_size", DEFAULT_FILE_SIZE_BYTES);
    if (file_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'file_size' must be a multiple of block_size",
            full_name,
            layout_type);

    size_t read_buffer_size = config.getInt64(dictionary_layout_prefix + ".read_buffer_size", DEFAULT_READ_BUFFER_SIZE_BYTES);
    if (read_buffer_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'read_buffer_size' must be a multiple of block_size",
            full_name,
            layout_type);

    size_t write_buffer_size = config.getInt64(dictionary_layout_prefix + ".write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE_BYTES);
    if (write_buffer_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'write_buffer_size' must be a multiple of block_size",
            full_name,
            layout_type);

    auto file_path = config.getString(dictionary_layout_prefix + ".path");
    if (file_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'path' must be specified",
            full_name,
            layout_type);

    SSDCacheDictionaryStorageConfiguration configuration
    {
        .strict_max_lifetime_seconds = strict_max_lifetime_seconds,
        .lifetime = dict_lifetime,
        .file_path = file_path,
        .max_partitions_count = max_partitions_count,
        .block_size = block_size,
        .file_blocks_size = file_size / block_size,
        .read_buffer_blocks_size = read_buffer_size / block_size,
        .write_buffer_blocks_size = write_buffer_size / block_size
    };

    return configuration;
}

#endif

static CacheDictionaryUpdateQueueConfiguration parseCacheDictionaryUpdateQueueConfiguration(
    const Poco::Util::AbstractConfiguration & config,
    const String & full_name,
    const String & layout_type,
    const String & dictionary_layout_prefix)
{
    size_t max_update_queue_size = config.getUInt64(dictionary_layout_prefix + ".max_update_queue_size", 100000);
    if (max_update_queue_size == 0)
        throw Exception(ErrorCodes::TOO_SMALL_BUFFER_SIZE,
            "{}: dictionary of layout '{}' setting 'max_update_queue_size' must be greater than 0",
            full_name,
            layout_type);

    size_t update_queue_push_timeout_milliseconds = config.getUInt64(dictionary_layout_prefix + ".update_queue_push_timeout_milliseconds", 10);
    if (update_queue_push_timeout_milliseconds < 10)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'update_queue_push_timeout_milliseconds' must be greater or equal than 10",
            full_name,
            layout_type);

    size_t query_wait_timeout_milliseconds = config.getUInt64(dictionary_layout_prefix + ".query_wait_timeout_milliseconds", 60000);

    size_t max_threads_for_updates = config.getUInt64(dictionary_layout_prefix + ".max_threads_for_updates", 4);
    if (max_threads_for_updates == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' setting 'max_threads_for_updates' must be greater than 0",
            full_name,
            layout_type);

    CacheDictionaryUpdateQueueConfiguration update_queue_configuration
    {
        .max_update_queue_size = max_update_queue_size,
        .max_threads_for_updates = max_threads_for_updates,
        .update_queue_push_timeout_milliseconds = update_queue_push_timeout_milliseconds,
        .query_wait_timeout_milliseconds = query_wait_timeout_milliseconds
    };

    return update_queue_configuration;
}

template <DictionaryKeyType dictionary_key_type, bool ssd>
DictionaryPtr createCacheDictionaryLayout(
    const String & full_name,
    const DictionaryStructure & dict_struct,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DictionarySourcePtr source_ptr,
    ContextPtr global_context [[maybe_unused]],
    bool created_from_ddl [[maybe_unused]])
{
    String layout_type;

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple && !ssd)
        layout_type = "cache";
    else if constexpr (dictionary_key_type == DictionaryKeyType::Simple && ssd)
        layout_type = "ssd_cache";
    else if constexpr (dictionary_key_type == DictionaryKeyType::Complex && !ssd)
        layout_type = "complex_key_cache";
    else if constexpr (dictionary_key_type == DictionaryKeyType::Complex && ssd)
        layout_type = "complex_key_ssd_cache";

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "{}: dictionary of layout '{}' 'key' is not supported", full_name, layout_type);
    }
    else if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "{}: dictionary of layout '{}' 'id' is not supported", full_name, layout_type);
    }

    if (dict_struct.range_min || dict_struct.range_max)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' elements .structure.range_min and .structure.range_max must be defined only "
            "for a dictionary of layout 'range_hashed'",
            full_name,
            layout_type);

    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
    if (require_nonempty)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: cache dictionary of layout '{}' cannot have 'require_nonempty' attribute set",
            full_name,
            layout_type);

    const auto dictionary_identifier = StorageID::fromDictionaryConfig(config, config_prefix);
    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

    const auto & layout_prefix = config_prefix + ".layout";
    const auto & dictionary_layout_prefix = layout_prefix + '.' + layout_type;
    const bool allow_read_expired_keys = config.getBool(dictionary_layout_prefix + ".allow_read_expired_keys", false);

    auto update_queue_configuration = parseCacheDictionaryUpdateQueueConfiguration(config, full_name, layout_type, dictionary_layout_prefix);

    std::shared_ptr<ICacheDictionaryStorage> storage;

    if constexpr (!ssd)
    {
        auto storage_configuration = parseCacheStorageConfiguration(config, full_name, layout_type, dictionary_layout_prefix, dict_lifetime);
        storage = std::make_shared<CacheDictionaryStorage<dictionary_key_type>>(dict_struct, storage_configuration);
    }
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_DARWIN)
    else
    {
        auto storage_configuration = parseSSDCacheStorageConfiguration(config, full_name, layout_type, dictionary_layout_prefix, dict_lifetime);
        auto user_files_path = global_context->getUserFilesPath();
        if (created_from_ddl && !pathStartsWith(storage_configuration.file_path, user_files_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", storage_configuration.file_path, user_files_path);

        storage = std::make_shared<SSDCacheDictionaryStorage<dictionary_key_type>>(storage_configuration);
    }
#endif
    ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
    const auto & settings = context->getSettingsRef();

    const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
    bool use_async_executor = clickhouse_source && clickhouse_source->isLocal() && settings[Setting::dictionary_use_async_executor];
    CacheDictionaryConfiguration configuration{
        allow_read_expired_keys,
        dict_lifetime,
        use_async_executor,
    };

    auto dictionary = std::make_unique<CacheDictionary<dictionary_key_type>>(
        dictionary_identifier,
        dict_struct,
        std::move(source_ptr),
        std::move(storage),
        update_queue_configuration,
        configuration);

    return dictionary;
}

void registerDictionaryCache(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory)
{
    auto create_simple_cache_layout = [=](const String & full_name,
                                          const DictionaryStructure & dict_struct,
                                          const Poco::Util::AbstractConfiguration & config,
                                          const std::string & config_prefix,
                                          DictionarySourcePtr source_ptr,
                                          ContextPtr global_context,
                                          bool created_from_ddl) -> DictionaryPtr
    {
        return createCacheDictionaryLayout<DictionaryKeyType::Simple, false/* ssd */>(full_name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
    };

    factory.registerLayout("cache", create_simple_cache_layout, false, true, Documentation{
        .description = R"DOCS_MD(
# cache dictionary layout

The `cached` dictionary layout type is stores the dictionary in a cache that has a fixed number of cells.
These cells contain frequently used elements.

The dictionary key has the [UInt64](/reference/data-types/int-uint) type.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

That applies to looking a key **up** - `dictGet` and the other dictionary functions. Reading the dictionary **as a table** with `SELECT ... FROM <dictionary>` is different: it returns the cells that happen to be resident in the cache at that moment and requests nothing from the source, because a cache keeps no record of which keys exist. A `WHERE` on the key is an ordinary filter over those resident cells, not a list of keys to fetch:

```sql
CREATE DICTIONARY cache_dict (id UInt64, data String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'cache_src')) LIFETIME(MIN 0 MAX 900) LAYOUT(CACHE(SIZE_IN_CELLS 1000));

-- nothing is cached yet, so nothing comes back and the source is not queried
SELECT count() FROM cache_dict WHERE id IN (1, 2, 3);
0

-- looking the keys up populates the cache
SELECT dictGet('cache_dict', 'data', toUInt64(number + 1)) FROM numbers(3);

-- and now the same read sees them
SELECT count() FROM cache_dict WHERE id IN (1, 2, 3);
3
```

So a cache dictionary is meant to be used through the dictionary functions. If you need `SELECT ... FROM <dictionary> WHERE key IN (...)` to fetch from the source, use the [direct](/sql-reference/statements/create/dictionary/layouts/direct) layout, which queries the source on every request and caches nothing; to read the whole dictionary as a table, use a layout that holds all of it, such as [flat](/sql-reference/statements/create/dictionary/layouts/flat) or [hashed](/sql-reference/statements/create/dictionary/layouts/hashed).

If keys are not found in dictionary, then update cache task is created and added into update queue. Update queue properties can be controlled with settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

For cache dictionaries, the expiration [lifetime](/reference/statements/create/dictionary/lifetime) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell's value is not used and key becomes expired. The key is re-requested the next time it needs to be used. This behaviour can be configured with setting `allow_read_expired_keys`.

This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the [system.dictionaries](/reference/system-tables/dictionaries) table.

If setting `allow_read_expired_keys` is set to 1, by default 0. Then dictionary can support asynchronous updates. If a client requests keys and all of them are in cache, but some of them are expired, then dictionary will return expired keys for a client and request them asynchronously from the source.

To improve cache performance, use a subquery with `LIMIT`, and call the function with the dictionary externally.

All types of sources are supported.

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
        <!-- Allows to read expired keys. -->
        <allow_read_expired_keys>0</allow_read_expired_keys>
        <!-- Max size of update queue. -->
        <max_update_queue_size>100000</max_update_queue_size>
        <!-- Max timeout in milliseconds for push update task into queue. -->
        <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
        <!-- Max wait timeout in milliseconds for update task to complete. -->
        <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
        <!-- Max threads for cache dictionary update. -->
        <max_threads_for_updates>4</max_threads_for_updates>
    </cache>
</layout>
```

</Tab>
</Tabs>
<br/>

Set a large enough cache size. You need to experiment to select the number of cells:

1.  Set some value.
2.  Run queries until the cache is completely full.
3.  Assess memory consumption using the `system.dictionaries` table.
4.  Increase or decrease the number of cells until the required memory consumption is reached.

<Note>
ClickHouse is not recommended as a source for this layout. Dictionary lookups require random point reads, which are not the access pattern ClickHouse is optimized for.
</Note>
)DOCS_MD",
        .syntax = "LAYOUT(CACHE(SIZE_IN_CELLS n))",
        .related = {"ssd_cache", "direct"}});

    auto create_complex_key_cache_layout = [=](const std::string & full_name,
                                               const DictionaryStructure & dict_struct,
                                               const Poco::Util::AbstractConfiguration & config,
                                               const std::string & config_prefix,
                                               DictionarySourcePtr source_ptr,
                                               ContextPtr global_context,
                                               bool created_from_ddl) -> DictionaryPtr
    {
        return createCacheDictionaryLayout<DictionaryKeyType::Complex, false /* ssd */>(full_name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
    };

    factory.registerLayout("complex_key_cache", create_complex_key_cache_layout, true, true, Documentation{
        .description = "Like `cache`, but supports composite keys. Reading it as a table returns only the cells "
                       "currently held in the cache and does not query the source; see `cache`.",
        .syntax = "LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS n))",
        .related = {"cache"}});

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_DARWIN)

    auto create_simple_ssd_cache_layout = [=](const std::string & full_name,
                                              const DictionaryStructure & dict_struct,
                                              const Poco::Util::AbstractConfiguration & config,
                                              const std::string & config_prefix,
                                              DictionarySourcePtr source_ptr,
                                              ContextPtr global_context,
                                              bool created_from_ddl) -> DictionaryPtr
    {
        return createCacheDictionaryLayout<DictionaryKeyType::Simple, true /* ssd */>(full_name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
    };

    factory.registerLayout("ssd_cache", create_simple_ssd_cache_layout, false, true, Documentation{
        .description = R"DOCS_MD(
# ssd_cache dictionary layout types

## ssd_cache {#ssd_cache}

Similar to `cache`, but stores data on SSD and index in RAM. All cache dictionary settings related to update queue can also be applied to SSD cache dictionaries.

Like `cache`, this layout keeps no record of which keys exist, so reading it as a table with `SELECT ... FROM <dictionary>` returns only the cells currently held and does not query the source. See [cache](/sql-reference/statements/create/dictionary/layouts/cache).

The dictionary key has the [UInt64](/reference/data-types/int-uint) type.

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH '/var/lib/clickhouse/user_files/test_dict'))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
    <ssd_cache>
        <!-- Size of elementary read block in bytes. Recommended to be equal to SSD's page size. -->
        <block_size>4096</block_size>
        <!-- Max cache file size in bytes. -->
        <file_size>16777216</file_size>
        <!-- Size of RAM buffer in bytes for reading elements from SSD. -->
        <read_buffer_size>131072</read_buffer_size>
        <!-- Size of RAM buffer in bytes for aggregating elements before flushing to SSD. -->
        <write_buffer_size>1048576</write_buffer_size>
        <!-- Path where cache file will be stored. -->
        <path>/var/lib/clickhouse/user_files/test_dict</path>
    </ssd_cache>
</layout>
```

</Tab>
</Tabs>
<br/>

## complex_key_ssd_cache {#complex_key_ssd_cache}

This type of storage is for use with composite [keys](/reference/statements/create/dictionary/attributes#composite-key). Similar to `ssd_cache`.
)DOCS_MD",
        .syntax = "LAYOUT(SSD_CACHE(PATH '/path/to/cache'))",
        .related = {"cache"}});

    auto create_complex_key_ssd_cache_layout = [=](const std::string & full_name,
                                                   const DictionaryStructure & dict_struct,
                                                   const Poco::Util::AbstractConfiguration & config,
                                                   const std::string & config_prefix,
                                                   DictionarySourcePtr source_ptr,
                                                   ContextPtr global_context,
                                                   bool created_from_ddl) -> DictionaryPtr {
        return createCacheDictionaryLayout<DictionaryKeyType::Complex, true /* ssd */>(full_name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
    };

    factory.registerLayout("complex_key_ssd_cache", create_complex_key_ssd_cache_layout, true, true, Documentation{
        .description = "Like `ssd_cache`, but supports composite keys.",
        .syntax = "LAYOUT(COMPLEX_KEY_SSD_CACHE(PATH '/path/to/cache'))",
        .related = {"ssd_cache"}});

#endif

}

}
