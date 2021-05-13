#include "CacheDictionary.h"
#include "CacheDictionaryStorage.h"
#include "SSDCacheDictionaryStorage.h"
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

CacheDictionaryStorageConfiguration parseCacheStorageConfiguration(
    const String & full_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & layout_prefix,
    const DictionaryLifetime & dict_lifetime,
    DictionaryKeyType dictionary_key_type)
{
    String dictionary_type_prefix = (dictionary_key_type == DictionaryKeyType::complex) ? ".complex_key_cache." : ".cache.";
    String dictionary_configuration_prefix = layout_prefix + dictionary_type_prefix;

    const size_t size = config.getUInt64(dictionary_configuration_prefix + "size_in_cells");
    if (size == 0)
        throw Exception(ErrorCodes::TOO_SMALL_BUFFER_SIZE,
            "{}: cache dictionary cannot have 0 cells",
            full_name);

    size_t dict_lifetime_seconds = static_cast<size_t>(dict_lifetime.max_sec);
    const size_t strict_max_lifetime_seconds = config.getUInt64(dictionary_configuration_prefix + "strict_max_lifetime_seconds", dict_lifetime_seconds);

    size_t rounded_size = roundUpToPowerOfTwoOrZero(size);

    CacheDictionaryStorageConfiguration storage_configuration{rounded_size, strict_max_lifetime_seconds, dict_lifetime};

    return storage_configuration;
}

#if defined(OS_LINUX) || defined(__FreeBSD__)

SSDCacheDictionaryStorageConfiguration parseSSDCacheStorageConfiguration(
    const String & full_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & layout_prefix,
    const DictionaryLifetime & dict_lifetime,
    DictionaryKeyType dictionary_key_type)
{
    String dictionary_type_prefix = dictionary_key_type == DictionaryKeyType::complex ? ".complex_key_ssd_cache." : ".ssd_cache.";
    String dictionary_configuration_prefix = layout_prefix + dictionary_type_prefix;

    const size_t strict_max_lifetime_seconds
        = config.getUInt64(dictionary_configuration_prefix + "strict_max_lifetime_seconds", static_cast<size_t>(dict_lifetime.max_sec));

    static constexpr size_t DEFAULT_SSD_BLOCK_SIZE_BYTES = DEFAULT_AIO_FILE_BLOCK_SIZE;
    static constexpr size_t DEFAULT_FILE_SIZE_BYTES = 4 * 1024 * 1024 * 1024ULL;
    static constexpr size_t DEFAULT_READ_BUFFER_SIZE_BYTES = 16 * DEFAULT_SSD_BLOCK_SIZE_BYTES;
    static constexpr size_t DEFAULT_WRITE_BUFFER_SIZE_BYTES = DEFAULT_SSD_BLOCK_SIZE_BYTES;

    static constexpr size_t DEFAULT_PARTITIONS_COUNT = 16;

    const size_t max_partitions_count
        = config.getInt64(dictionary_configuration_prefix + "ssd_cache.max_partitions_count", DEFAULT_PARTITIONS_COUNT);

    const size_t block_size = config.getInt64(dictionary_configuration_prefix + "block_size", DEFAULT_SSD_BLOCK_SIZE_BYTES);
    const size_t file_size = config.getInt64(dictionary_configuration_prefix + "file_size", DEFAULT_FILE_SIZE_BYTES);
    if (file_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: file_size must be a multiple of block_size",
            full_name);

    const size_t read_buffer_size = config.getInt64(dictionary_configuration_prefix + "read_buffer_size", DEFAULT_READ_BUFFER_SIZE_BYTES);
    if (read_buffer_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: read_buffer_size must be a multiple of block_size",
            full_name);

    const size_t write_buffer_size
        = config.getInt64(dictionary_configuration_prefix + "write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE_BYTES);
    if (write_buffer_size % block_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: write_buffer_size must be a multiple of block_size",
            full_name);

    auto directory_path = config.getString(dictionary_configuration_prefix + "path");
    if (directory_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: ssd cache dictionary cannot have empty path",
            full_name);

    if (directory_path.at(0) != '/')
        directory_path = std::filesystem::path{config.getString("path")}.concat(directory_path).string();

    SSDCacheDictionaryStorageConfiguration configuration{
        strict_max_lifetime_seconds,
        dict_lifetime,
        directory_path,
        max_partitions_count,
        block_size,
        file_size / block_size,
        read_buffer_size / block_size,
        write_buffer_size / block_size};

    return configuration;
}

#endif

CacheDictionaryUpdateQueueConfiguration parseCacheDictionaryUpdateQueueConfiguration(
    const String & full_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & layout_prefix,
    DictionaryKeyType key_type)
{
    String layout_type = key_type == DictionaryKeyType::complex ? "complex_key_cache" : "cache";

    const size_t max_update_queue_size = config.getUInt64(layout_prefix + ".cache.max_update_queue_size", 100000);
    if (max_update_queue_size == 0)
        throw Exception(ErrorCodes::TOO_SMALL_BUFFER_SIZE,
            "{}: dictionary of layout '{}' cannot have empty update queue of size 0",
            full_name,
            layout_type);

    const size_t update_queue_push_timeout_milliseconds
        = config.getUInt64(layout_prefix + ".cache.update_queue_push_timeout_milliseconds", 10);
    if (update_queue_push_timeout_milliseconds < 10)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' have too little update_queue_push_timeout",
            full_name,
            layout_type);

    const size_t query_wait_timeout_milliseconds = config.getUInt64(layout_prefix + ".cache.query_wait_timeout_milliseconds", 60000);

    const size_t max_threads_for_updates = config.getUInt64(layout_prefix + ".max_threads_for_updates", 4);
    if (max_threads_for_updates == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout) '{}' cannot have zero threads for updates",
            full_name,
            layout_type);

    CacheDictionaryUpdateQueueConfiguration update_queue_configuration{
        max_update_queue_size, max_threads_for_updates, update_queue_push_timeout_milliseconds, query_wait_timeout_milliseconds};

    return update_queue_configuration;
}

template <DictionaryKeyType dictionary_key_type>
DictionaryPtr createCacheDictionaryLayout(
    const String & full_name,
    const DictionaryStructure & dict_struct,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DictionarySourcePtr source_ptr)
{
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionary");

    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'cache'");
    }
    else if constexpr (dictionary_key_type == DictionaryKeyType::complex)
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_cache'");
    }

    if (dict_struct.range_min || dict_struct.range_max)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: elements .structure.range_min and .structure.range_max should be defined only "
            "for a dictionary of layout 'range_hashed'",
            full_name);

    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
    if (require_nonempty)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: cache dictionary of layout cannot have 'require_nonempty' attribute set",
            full_name);

    const auto & layout_prefix = config_prefix + ".layout";

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

    const bool allow_read_expired_keys = config.getBool(layout_prefix + ".cache.allow_read_expired_keys", false);

    auto storage_configuration = parseCacheStorageConfiguration(full_name, config, layout_prefix, dict_lifetime, dictionary_key_type);

    std::shared_ptr<ICacheDictionaryStorage> storage = std::make_shared<CacheDictionaryStorage<dictionary_key_type>>(dict_struct, storage_configuration);

    auto update_queue_configuration = parseCacheDictionaryUpdateQueueConfiguration(full_name, config, layout_prefix, dictionary_key_type);

    return std::make_unique<CacheDictionary<dictionary_key_type>>(
        dict_id, dict_struct, std::move(source_ptr), storage, update_queue_configuration, dict_lifetime, allow_read_expired_keys);
}

#if defined(OS_LINUX) || defined(__FreeBSD__)

template <DictionaryKeyType dictionary_key_type>
DictionaryPtr createSSDCacheDictionaryLayout(
    const String & full_name,
    const DictionaryStructure & dict_struct,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DictionarySourcePtr source_ptr)
{
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionary");

    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'ssd_cache'");
    }
    else if constexpr (dictionary_key_type == DictionaryKeyType::complex)
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_ssd_cache'");
    }

    if (dict_struct.range_min || dict_struct.range_max)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: elements .structure.range_min and .structure.range_max should be defined only "
            "for a dictionary of layout 'range_hashed'",
            full_name);

    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
    if (require_nonempty)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{}: cache dictionary of layout cannot have 'require_nonempty' attribute set",
            full_name);

    const auto & layout_prefix = config_prefix + ".layout";

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

    const bool allow_read_expired_keys = config.getBool(layout_prefix + ".cache.allow_read_expired_keys", false);

    auto storage_configuration = parseSSDCacheStorageConfiguration(full_name, config, layout_prefix, dict_lifetime, dictionary_key_type);
    auto storage = std::make_shared<SSDCacheDictionaryStorage<dictionary_key_type>>(storage_configuration);

    auto update_queue_configuration
        = parseCacheDictionaryUpdateQueueConfiguration(full_name, config, layout_prefix, dictionary_key_type);

    return std::make_unique<CacheDictionary<dictionary_key_type>>(
        dict_id, dict_struct, std::move(source_ptr), storage, update_queue_configuration, dict_lifetime, allow_read_expired_keys);
}

#endif

void registerDictionaryCache(DictionaryFactory & factory)
{
    auto create_simple_cache_layout = [=](const String & full_name,
                                          const DictionaryStructure & dict_struct,
                                          const Poco::Util::AbstractConfiguration & config,
                                          const std::string & config_prefix,
                                          DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        return createCacheDictionaryLayout<DictionaryKeyType::simple>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("cache", create_simple_cache_layout, false);

    auto create_complex_key_cache_layout = [=](const std::string & full_name,
                                               const DictionaryStructure & dict_struct,
                                               const Poco::Util::AbstractConfiguration & config,
                                               const std::string & config_prefix,
                                               DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        return createCacheDictionaryLayout<DictionaryKeyType::complex>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("complex_key_cache", create_complex_key_cache_layout, true);

#if defined(OS_LINUX) || defined(__FreeBSD__)

    auto create_simple_ssd_cache_layout = [=](const std::string & full_name,
                                              const DictionaryStructure & dict_struct,
                                              const Poco::Util::AbstractConfiguration & config,
                                              const std::string & config_prefix,
                                              DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        return createSSDCacheDictionaryLayout<DictionaryKeyType::simple>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("ssd_cache", create_simple_ssd_cache_layout, false);

    auto create_complex_key_ssd_cache_layout = [=](const std::string & full_name,
                                                   const DictionaryStructure & dict_struct,
                                                   const Poco::Util::AbstractConfiguration & config,
                                                   const std::string & config_prefix,
                                                   DictionarySourcePtr source_ptr) -> DictionaryPtr {
        return createSSDCacheDictionaryLayout<DictionaryKeyType::complex>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("complex_key_ssd_cache", create_complex_key_ssd_cache_layout, true);
#endif
}

}
