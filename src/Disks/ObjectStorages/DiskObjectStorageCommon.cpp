#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Common/getRandomASCIIString.h>
#include <Common/FileCacheFactory.h>
#include <Common/FileCache.h>
#include <Common/FileCacheSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::shared_ptr<DiskCacheWrapper> wrapWithCache(
    std::shared_ptr<IDisk> disk, String cache_name, String cache_path, String metadata_path)
{
    if (metadata_path == cache_path)
        throw Exception("Metadata and cache paths should be different: " + metadata_path, ErrorCodes::BAD_ARGUMENTS);

    auto cache_disk = std::make_shared<DiskLocal>(cache_name, cache_path, 0);
    auto cache_file_predicate = [] (const String & path)
    {
        return path.ends_with("idx") // index files.
                || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
                || path.ends_with("txt") || path.ends_with("dat");
    };

    return std::make_shared<DiskCacheWrapper>(disk, cache_disk, cache_file_predicate);
}

static String getDiskMetadataPath(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    return config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
}

std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    /// where the metadata files are stored locally
    auto metadata_path = getDiskMetadataPath(name, config, config_prefix, context);
    fs::create_directories(metadata_path);
    auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0);
    return std::make_pair(metadata_path, metadata_disk);
}

FileCachePtr getCachePtrForDisk(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    if (!config.has(config_prefix + ".cache"))
        return nullptr;

    auto default_cache_base_path = config.getString(config_prefix + ".data_cache_path", fs::path(context->getPath()) / "disks" / name / "data_cache/");
    auto metadata_path = getDiskMetadataPath(name, config, config_prefix, context);

    if (metadata_path == default_cache_base_path)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata path and cache base path must be different: {}", metadata_path);

    auto cache_name = config.getString(config_prefix + ".cache");
    auto cache_config_prefix = "storage_configuration.cache." + cache_name;

    FileCacheSettings file_cache_settings;
    file_cache_settings.loadFromConfig(config, cache_config_prefix, default_cache_base_path);

    if (!fs::exists(file_cache_settings.cache_base_path))
        fs::create_directories(file_cache_settings.cache_base_path);

    auto cache = FileCacheFactory::instance().getOrCreate(file_cache_settings.cache_base_path, file_cache_settings);
    cache->initialize();

    auto * log = &Poco::Logger::get("Disk(" + name + ")");
    LOG_INFO(log, "Disk registered with prefix [{}], cache_prefix [{}]", config_prefix, cache_config_prefix);

    LOG_INFO(log, "Disk registered with cache path: {}. Cache size: {}, max cache elements size: {}, max_file_segment_size: {}, enable_cache_hits_threshold: {}",
             file_cache_settings.cache_base_path,
             file_cache_settings.max_size ? toString(file_cache_settings.max_size) : "UNLIMITED",
             file_cache_settings.max_elements ? toString(file_cache_settings.max_elements) : "UNLIMITED",
             file_cache_settings.max_file_segment_size,
             file_cache_settings.enable_cache_hits_threshold);

    return cache;
}

}
