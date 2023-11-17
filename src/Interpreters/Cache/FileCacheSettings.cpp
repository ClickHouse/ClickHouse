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

void FileCacheSettings::loadImpl(FuncHas has, FuncGetUInt get_uint, FuncGetString get_string)
{
    auto config_parse_size = [&](std::string_view key) { return parseWithSizeSuffix<uint64_t>(get_string(key)); };

    if (!has("path"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache path (`path`) in configuration");

    base_path = get_string("path");

    if (!has("max_size"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected cache size (`max_size`) in configuration");

    max_size = config_parse_size("max_size");
    if (max_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected non-zero size for cache configuration");

    if (has("max_elements"))
        max_elements = get_uint("max_elements");

    if (has("max_file_segment_size"))
        max_file_segment_size = config_parse_size("max_file_segment_size");

    if (has("cache_on_write_operations"))
        cache_on_write_operations = get_uint("cache_on_write_operations");

    if (has("enable_filesystem_query_cache_limit"))
        enable_filesystem_query_cache_limit = get_uint("enable_filesystem_query_cache_limit");

    if (has("cache_hits_threshold"))
        cache_hits_threshold = get_uint("cache_hits_threshold");

    if (has("enable_bypass_cache_with_threshold"))
        enable_bypass_cache_with_threshold = get_uint("enable_bypass_cache_with_threshold");

    if (has("bypass_cache_threshold"))
        bypass_cache_threshold = config_parse_size("bypass_cache_threshold");

    if (has("boundary_alignment"))
        boundary_alignment = config_parse_size("boundary_alignment");

    if (has("background_download_threads"))
        background_download_threads = get_uint("background_download_threads");

    if (has("load_metadata_threads"))
        load_metadata_threads = get_uint("load_metadata_threads");
}

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto config_has = [&](std::string_view key) { return config.has(fmt::format("{}.{}", config_prefix, key)); };
    auto config_get_uint = [&](std::string_view key) { return config.getUInt(fmt::format("{}.{}", config_prefix, key)); };
    auto config_get_string = [&](std::string_view key) { return config.getString(fmt::format("{}.{}", config_prefix, key)); };
    loadImpl(std::move(config_has), std::move(config_get_uint), std::move(config_get_string));
}

void FileCacheSettings::loadFromCollection(const NamedCollection & collection)
{
    auto config_has = [&](std::string_view key) { return collection.has(std::string(key)); };
    auto config_get_uint = [&](std::string_view key) { return collection.get<UInt64>(std::string(key)); };
    auto config_get_string = [&](std::string_view key) { return collection.get<String>(std::string(key)); };
    loadImpl(std::move(config_has), std::move(config_get_uint), std::move(config_get_string));
}

}
