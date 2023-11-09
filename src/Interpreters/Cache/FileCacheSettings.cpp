#include "FileCacheSettings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".path"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache path (`path`) in configuration");

    base_path = config.getString(config_prefix + ".path");

    if (!config.has(config_prefix + ".max_size"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache size (`max_size`) in configuration");

    max_size = parseWithSizeSuffix<uint64_t>(config.getString(config_prefix + ".max_size"));
    if (max_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected non-zero size for cache configuration");

    max_elements = config.getUInt64(config_prefix + ".max_elements", FILECACHE_DEFAULT_MAX_ELEMENTS);

    if (config.has(config_prefix + ".max_file_segment_size"))
        max_file_segment_size = parseWithSizeSuffix<uint64_t>(config.getString(config_prefix + ".max_file_segment_size"));

    cache_on_write_operations = config.getUInt64(config_prefix + ".cache_on_write_operations", false);
    enable_filesystem_query_cache_limit = config.getUInt64(config_prefix + ".enable_filesystem_query_cache_limit", false);
    cache_hits_threshold = config.getUInt64(config_prefix + ".cache_hits_threshold", FILECACHE_DEFAULT_HITS_THRESHOLD);
    enable_bypass_cache_with_threshold = config.getUInt64(config_prefix + ".enable_bypass_cache_with_threshold", false);

    if (config.has(config_prefix + ".bypass_cache_threshold"))
        bypass_cache_threshold = parseWithSizeSuffix<uint64_t>(config.getString(config_prefix + ".bypass_cache_threshold"));

    if (config.has(config_prefix + ".boundary_alignment"))
        boundary_alignment = parseWithSizeSuffix<uint64_t>(config.getString(config_prefix + ".boundary_alignment"));

    if (config.has(config_prefix + ".background_download_threads"))
        background_download_threads = config.getUInt(config_prefix + ".background_download_threads");

    if (config.has(config_prefix + ".load_metadata_threads"))
        load_metadata_threads = config.getUInt(config_prefix + ".load_metadata_threads");
}

void FileCacheSettings::loadFromCollection(const NamedCollection & collection)
{
    if (!collection.has("path"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache path (`path`) in configuration");

    base_path = collection.get<String>("path");

    if (!collection.has("max_size"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache size (`max_size`) in configuration");

    max_size = parseWithSizeSuffix<uint64_t>(collection.get<String>("max_size"));
    if (max_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected non-zero size for cache configuration");

    max_elements = collection.getOrDefault<UInt64>("max_elements", FILECACHE_DEFAULT_MAX_ELEMENTS);

    if (collection.has("max_file_segment_size"))
        max_file_segment_size = parseWithSizeSuffix<uint64_t>(collection.get<String>("max_file_segment_size"));

    cache_on_write_operations = collection.getOrDefault<UInt64>("cache_on_write_operations", false);
    enable_filesystem_query_cache_limit = collection.getOrDefault<UInt64>("enable_filesystem_query_cache_limit", false);
    cache_hits_threshold = collection.getOrDefault<UInt64>("cache_hits_threshold", FILECACHE_DEFAULT_HITS_THRESHOLD);
    enable_bypass_cache_with_threshold = collection.getOrDefault<UInt64>("enable_bypass_cache_with_threshold", false);

    if (collection.has("bypass_cache_threshold"))
        bypass_cache_threshold = parseWithSizeSuffix<uint64_t>(collection.get<String>("bypass_cache_threshold"));

    if (collection.has("boundary_alignment"))
        boundary_alignment = parseWithSizeSuffix<uint64_t>(collection.get<String>("boundary_alignment"));

    if (collection.has("background_download_threads"))
        background_download_threads = collection.get<UInt64>("background_download_threads");

    if (collection.has("load_metadata_threads"))
        load_metadata_threads = collection.get<UInt64>("load_metadata_threads");
}

}
