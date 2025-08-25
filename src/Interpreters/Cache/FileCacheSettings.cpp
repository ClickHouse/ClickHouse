#include "FileCacheSettings.h"

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/logger_useful.h>
#include <Common/filesystemHelpers.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Interpreters/Cache/FileCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

#define LIST_OF_FILE_CACHE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, path, "", "Cache directory path", 0) \
    DECLARE(UInt64, max_size, 0, "Maximum cache size", 0) \
    DECLARE(UInt64, max_elements, FILECACHE_DEFAULT_MAX_ELEMENTS, "Maximum number of cache elements, e.g. file segments (limits number of files on filesystem)", 0) \
    DECLARE(UInt64, max_file_segment_size, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE, "Maximum size of a single file segment", 0) \
    DECLARE(UInt64, boundary_alignment, FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT, "File segment alignment", 0) \
    DECLARE(Bool, cache_on_write_operations, false, "Enables write-through cache (cache on INSERT and MERGE)", 0) \
    DECLARE(FileCachePolicy, cache_policy, FILECACHE_DEFAULT_CACHE_POLICY, "Cache eviction policy", 0) \
    DECLARE(Double, slru_size_ratio, FILECACHE_DEFAULT_SLRU_RATIO, "SLRU cache policy size ratio of protected to probationary elements", 0) \
    DECLARE(UInt64, background_download_threads, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS, "Number of background download threads. Value 0 disables background download", 0) \
    DECLARE(UInt64, background_download_queue_size_limit, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT, "Size of background download queue. Value 0 disables background download", 0) \
    DECLARE(UInt64, background_download_max_file_segment_size, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD, "Maximum size which can be downloaded in background download", 0) \
    DECLARE(UInt64, load_metadata_threads, FILECACHE_DEFAULT_LOAD_METADATA_THREADS, "Number of threads to load cache metadata at server startup. Value 0 disables asynchronous loading of metadata", 0) \
    DECLARE(Bool, load_metadata_asynchronously, false, "Enables asynchronous loading of metadata on server startup", 0) \
    DECLARE(Double, keep_free_space_size_ratio, FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO, "A ratio of free space which cache would try to uphold in the background", 0) \
    DECLARE(Double, keep_free_space_elements_ratio, FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO, "A ratio of free elements which cache would try to uphold in the background", 0) \
    DECLARE(UInt64, keep_free_space_remove_batch, FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH, "A remove batch size of cache elements made by background thread which upholds free space/elements ratio", 0) \
    DECLARE(Bool, enable_filesystem_query_cache_limit, false, "Enable limiting maximum size of cache which can be written within a query", 0) \
    DECLARE(UInt64, cache_hits_threshold, FILECACHE_DEFAULT_HITS_THRESHOLD, "Number of cache hits required to cache corresponding file segment", 0) \
    DECLARE(Bool, enable_bypass_cache_with_threshold, false, "Undocumented. Not recommended for use", 0) \
    DECLARE(UInt64, bypass_cache_threshold, FILECACHE_BYPASS_THRESHOLD, "Undocumented. Not recommended for use", 0) \
    DECLARE(Bool, write_cache_per_user_id_directory, false, "Internal ClickHouse Cloud setting", 0) \
    DECLARE(Bool, allow_dynamic_cache_resize, false, "Allow dynamic resize of filesystem cache", 0) \
    DECLARE(Double, max_size_ratio_to_total_space, 0, "Ratio of `max_size` to total disk space", 0) \

DECLARE_SETTINGS_TRAITS(FileCacheSettingsTraits, LIST_OF_FILE_CACHE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(FileCacheSettingsTraits, LIST_OF_FILE_CACHE_SETTINGS)

struct FileCacheSettingsImpl : public BaseSettings<FileCacheSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    FileCacheSettings##TYPE NAME = &FileCacheSettingsImpl ::NAME;

namespace FileCacheSetting
{
LIST_OF_FILE_CACHE_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

FileCacheSettings::FileCacheSettings() : impl(std::make_unique<FileCacheSettingsImpl>())
{
}

FileCacheSettings::~FileCacheSettings() = default;

FILE_CACHE_SETTINGS_SUPPORTED_TYPES(FileCacheSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


FileCacheSettings::FileCacheSettings(const FileCacheSettings & settings)
    : impl(std::make_unique<FileCacheSettingsImpl>(*settings.impl))
{
}

FileCacheSettings::FileCacheSettings(FileCacheSettings && settings) noexcept
    : impl(std::make_unique<FileCacheSettingsImpl>(std::move(*settings.impl)))
{
}

FileCacheSettings & FileCacheSettings::operator=(FileCacheSettings && settings) noexcept
{
    impl = std::make_unique<FileCacheSettingsImpl>(std::move(*settings.impl));
    return *this;
}

bool FileCacheSettings::operator==(const FileCacheSettings & settings) const noexcept
{
    return *impl == *settings.impl;
}

ColumnsDescription FileCacheSettings::getColumnsDescription()
{
    FileCacheSettingsImpl impl;
    ColumnsDescription result;

    result.add(
        ColumnDescription(
            "cache_name", std::make_shared<DataTypeString>(), "Cache name"));

    for (const auto & setting : impl.all())
    {
        ColumnDescription desc;
        desc.name = setting.getName();
        desc.type = [&]() -> DataTypePtr
        {
            const std::string type_name = setting.getTypeName();
            if (type_name == "UInt64")
                return std::make_shared<DataTypeUInt64>();
            else if (type_name == "String")
                return std::make_shared<DataTypeString>();
            else if (type_name == "Bool")
                return std::make_shared<DataTypeUInt8>();
            else if (type_name == "Double")
                return std::make_shared<DataTypeFloat64>();
            else if (type_name == "FileCachePolicy")
                return std::make_shared<DataTypeString>();
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", type_name);
        }();
        desc.comment = setting.getDescription();
        result.add(desc);
    }

    result.add(
        ColumnDescription(
            "is_initialized", std::make_shared<DataTypeUInt8>(), "Indicates whether cache was successfully initialized"));

    result.add(
        ColumnDescription(
            "current_size", std::make_shared<DataTypeUInt64>(), "Current cache size"));
    result.add(
        ColumnDescription(
            "current_elements_num", std::make_shared<DataTypeUInt64>(), "Current cache elements (file segments) number"));

    return result;
}

void FileCacheSettings::dumpToSystemSettingsColumns(
    MutableColumnsAndConstraints & params,
    const std::string & cache_name,
    const FileCachePtr & cache) const
{
    MutableColumns & res_columns = params.res_columns;
    size_t i = 0;
    res_columns[i++]->insert(cache_name);

    for (const auto & setting : impl->all())
        res_columns[i++]->insert(setting.getValue());

    res_columns[i++]->insert(cache->isInitialized());
    res_columns[i++]->insert(cache->getUsedCacheSize());
    res_columns[i++]->insert(cache->getFileSegmentsNum());
}

void FileCacheSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const std::string & cache_path_prefix_if_relative,
    const std::string & default_cache_path)
{
    if (!config.has(config_prefix))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}' in configuration file.", config_prefix);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    std::set<std::string> ignore_keys = {"type", "disk", "name"};
    for (const std::string & key : config_keys)
    {
        if (ignore_keys.contains(key))
            continue;
        impl->set(key, config.getString(config_prefix + "." + key));
    }

    if ((*this)[FileCacheSetting::path].changed)
    {
        if (fs::path((*this)[FileCacheSetting::path].value).is_relative())
        {
            if (cache_path_prefix_if_relative.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache path prefix for relative paths was not provided");

            is_path_relative_in_config = true;
            (*this)[FileCacheSetting::path] = fs::path(cache_path_prefix_if_relative) / (*this)[FileCacheSetting::path].value;
        }
    }
    else
    {
        (*this)[FileCacheSetting::path] = default_cache_path;
    }

    validate();
}

void FileCacheSettings::loadFromCollection(
    const NamedCollection & collection,
    const std::string & cache_path_prefix_if_relative)
{
    for (const auto & key : collection.getKeys())
    {
        impl->set(key, collection.get<String>(key));
    }

    if (fs::path((*this)[FileCacheSetting::path].value).is_relative())
    {
        if (cache_path_prefix_if_relative.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache path prefix for relative paths was not provided");

        is_path_relative_in_config = true;
        (*this)[FileCacheSetting::path] = fs::path(cache_path_prefix_if_relative) / (*this)[FileCacheSetting::path].value;
    }

    validate();
}

void FileCacheSettings::validate()
{
    auto & settings = *this;

    if (!settings[FileCacheSetting::path].changed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`path` is required parameter of cache configuration");

    if (fs::path((*this)[FileCacheSetting::path].value).is_relative())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`path` was not normalized to absolute");

    if (!settings[FileCacheSetting::max_size].changed && !settings[FileCacheSetting::max_size_ratio_to_total_space].changed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either `max_size` or `max_size_ratio_to_total_space` must be defined in cache configuration");

    if (settings[FileCacheSetting::max_size].changed && settings[FileCacheSetting::max_size_ratio_to_total_space].changed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`max_size` and `max_size_ratio_to_total_space` cannot be specified at the same time");

    if (settings[FileCacheSetting::max_size].changed && settings[FileCacheSetting::max_size] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`max_size` cannot be 0");

    if (settings[FileCacheSetting::max_size_ratio_to_total_space].changed)
    {
        if (settings[FileCacheSetting::max_size_ratio_to_total_space] <= 0 || settings[FileCacheSetting::max_size_ratio_to_total_space] > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`max_size_ratio_to_total_space` must be in range (0, 1]");

        fs::create_directories(settings[FileCacheSetting::path].value);
        struct statvfs stat = getStatVFS(settings[FileCacheSetting::path]);
        const auto total_space = stat.f_blocks * stat.f_frsize;
        settings[FileCacheSetting::max_size] =
            static_cast<UInt64>(std::floor(settings[FileCacheSetting::max_size_ratio_to_total_space].value * total_space));

        LOG_INFO(
            getLogger("FileCacheSettings"),
            "Using max_size as ratio {} to total disk space on path {}: {} (total space: {})",
            settings[FileCacheSetting::max_size_ratio_to_total_space].value,
            settings[FileCacheSetting::path].value,
            settings[FileCacheSetting::max_size].value, total_space);
    }
}

}
