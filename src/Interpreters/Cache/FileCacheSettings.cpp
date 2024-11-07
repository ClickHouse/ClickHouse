#include "FileCacheSettings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void FileCacheSettings::loadImpl(FuncHas has, FuncGetUInt get_uint, FuncGetString get_string, FuncGetDouble get_double)
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

    if (has("background_download_queue_size_limit"))
        background_download_queue_size_limit = get_uint("background_download_queue_size_limit");

    if (has("load_metadata_threads"))
        load_metadata_threads = get_uint("load_metadata_threads");

    if (has("load_metadata_asynchronously"))
        load_metadata_asynchronously = get_uint("load_metadata_asynchronously");

    if (boundary_alignment > max_file_segment_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `boundary_alignment` cannot exceed `max_file_segment_size`");

    if (has("cache_policy"))
    {
        cache_policy = get_string("cache_policy");
        boost::to_upper(cache_policy);
    }

    if (has("slru_size_ratio"))
        slru_size_ratio = get_double("slru_size_ratio");

    if (has("write_cache_per_user_id_directory"))
        slru_size_ratio = get_uint("write_cache_per_user_id_directory");

    if (has("keep_free_space_size_ratio"))
        keep_free_space_size_ratio = get_double("keep_free_space_size_ratio");

    if (has("keep_free_space_elements_ratio"))
        keep_free_space_elements_ratio = get_double("keep_free_space_elements_ratio");

    if (has("keep_free_space_remove_batch"))
        keep_free_space_elements_ratio = get_uint("keep_free_space_remove_batch");
}

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto config_has = [&](std::string_view key) { return config.has(fmt::format("{}.{}", config_prefix, key)); };
    auto config_get_uint = [&](std::string_view key) { return config.getUInt(fmt::format("{}.{}", config_prefix, key)); };
    auto config_get_string = [&](std::string_view key) { return config.getString(fmt::format("{}.{}", config_prefix, key)); };
    auto config_get_double = [&](std::string_view key) { return config.getDouble(fmt::format("{}.{}", config_prefix, key)); };
    loadImpl(std::move(config_has), std::move(config_get_uint), std::move(config_get_string), std::move(config_get_double));
}

void FileCacheSettings::loadFromCollection(const NamedCollection & collection)
{
    auto collection_has = [&](std::string_view key) { return collection.has(std::string(key)); };
    auto collection_get_uint = [&](std::string_view key) { return collection.get<UInt64>(std::string(key)); };
    auto collection_get_string = [&](std::string_view key) { return collection.get<String>(std::string(key)); };
    auto collection_get_double = [&](std::string_view key) { return collection.get<Float64>(std::string(key)); };
    loadImpl(std::move(collection_has), std::move(collection_get_uint), std::move(collection_get_string), std::move(collection_get_double));
}

std::string FileCacheSettings::toString() const
{
    WriteBufferFromOwnString res;
    res << "base_path: " << base_path << ", ";
    res << "max_size: " << max_size << ", ";
    res << "max_elements: " << max_elements << ", ";
    res << "max_file_segment_size: " << max_file_segment_size << ", ";
    res << "cache_on_write_operations: " << cache_on_write_operations << ", ";
    res << "cache_hits_threshold: " << cache_hits_threshold << ", ";
    res << "enable_filesystem_query_cache_limit: " << enable_filesystem_query_cache_limit << ", ";
    res << "bypass_cache_threshold: " << bypass_cache_threshold << ", ";
    res << "boundary_alignment: " << boundary_alignment << ", ";
    res << "background_download_threads: " << background_download_threads << ", ";
    res << "background_download_queue_size_limit: " << background_download_queue_size_limit << ", ";
    res << "load_metadata_threads: " << load_metadata_threads << ", ";
    res << "write_cache_per_user_id_directory: " << write_cache_per_user_id_directory << ", ";
    res << "cache_policy: " << cache_policy << ", ";
    res << "slru_size_ratio: " << slru_size_ratio << ", ";
    return res.str();
}

std::vector<std::string> FileCacheSettings::getSettingsDiff(const FileCacheSettings & other) const
{
    std::vector<std::string> res;
    if (base_path != other.base_path)
        res.push_back("base_path");
    if (max_size != other.max_size)
        res.push_back("max_size");
    if (max_elements != other.max_elements)
        res.push_back("max_elements");
    if (max_file_segment_size != other.max_file_segment_size)
        res.push_back("max_file_segment_size");
    if (cache_on_write_operations != other.cache_on_write_operations)
        res.push_back("cache_on_write_operations");
    if (cache_hits_threshold != other.cache_hits_threshold)
        res.push_back("cache_hits_threshold");
    if (enable_filesystem_query_cache_limit != other.enable_filesystem_query_cache_limit)
        res.push_back("enable_filesystem_query_cache_limit");
    if (bypass_cache_threshold != other.bypass_cache_threshold)
        res.push_back("bypass_cache_threshold");
    if (boundary_alignment != other.boundary_alignment)
        res.push_back("boundary_alignment");
    if (background_download_threads != other.background_download_threads)
        res.push_back("background_download_threads");
    if (background_download_queue_size_limit != other.background_download_queue_size_limit)
        res.push_back("background_download_queue_size_limit");
    if (load_metadata_threads != other.load_metadata_threads)
        res.push_back("load_metadata_threads");
    if (write_cache_per_user_id_directory != other.write_cache_per_user_id_directory)
        res.push_back("write_cache_per_user_directory");
    if (cache_policy != other.cache_policy)
        res.push_back("cache_policy");
    if (slru_size_ratio != other.slru_size_ratio)
        res.push_back("slru_size_ratio");
    return res;
}

}
