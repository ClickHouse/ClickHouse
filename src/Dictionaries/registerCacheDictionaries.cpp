#include "CacheDictionary.h"
#include "CacheDictionaryStorage.h"
#include "SSDCacheDictionaryStorage.h"
#include <Common/filesystemHelpers.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

CacheDictionaryStorageConfiguration parseCacheStorageConfiguration(
    const Poco::Util::AbstractConfiguration & config,
    const String & full_name,
    const String & layout_type,
    const String & dictionary_layout_prefix,
    const DictionaryLifetime & dict_lifetime)
{
    size_t size = config.getUInt64(dictionary_layout_prefix + ".size_in_cells");
    if (size == 0)
        throw Exception(ErrorCodes::TOO_SMALL_BUFFER_SIZE, "{}: dictionary of layout '{}' setting 'size_in_cells' must be greater than 0", full_name, layout_type);

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

#if defined(OS_LINUX) || defined(OS_FREEBSD)

SSDCacheDictionaryStorageConfiguration parseSSDCacheStorageConfiguration(
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

CacheDictionaryUpdateQueueConfiguration parseCacheDictionaryUpdateQueueConfiguration(
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
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    else
    {
        auto storage_configuration = parseSSDCacheStorageConfiguration(config, full_name, layout_type, dictionary_layout_prefix, dict_lifetime);
        auto user_files_path = global_context->getUserFilesPath();
        if (created_from_ddl && !pathStartsWith(storage_configuration.file_path, user_files_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", storage_configuration.file_path, user_files_path);

        storage = std::make_shared<SSDCacheDictionaryStorage<dictionary_key_type>>(storage_configuration);
    }
#endif

    auto dictionary = std::make_unique<CacheDictionary<dictionary_key_type>>(
        dictionary_identifier,
        dict_struct,
        std::move(source_ptr),
        std::move(storage),
        update_queue_configuration,
        dict_lifetime,
        allow_read_expired_keys);

    return dictionary;
}

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

    factory.registerLayout("cache", create_simple_cache_layout, false);

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

    factory.registerLayout("complex_key_cache", create_complex_key_cache_layout, true);

#if defined(OS_LINUX) || defined(OS_FREEBSD)

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

    factory.registerLayout("ssd_cache", create_simple_ssd_cache_layout, false);

    auto create_complex_key_ssd_cache_layout = [=](const std::string & full_name,
                                                   const DictionaryStructure & dict_struct,
                                                   const Poco::Util::AbstractConfiguration & config,
                                                   const std::string & config_prefix,
                                                   DictionarySourcePtr source_ptr,
                                                   ContextPtr global_context,
                                                   bool created_from_ddl) -> DictionaryPtr {
        return createCacheDictionaryLayout<DictionaryKeyType::Complex, true /* ssd */>(full_name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
    };

    factory.registerLayout("complex_key_ssd_cache", create_complex_key_ssd_cache_layout, true);

#endif

}

}
