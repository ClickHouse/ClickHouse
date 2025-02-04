#include "FileCacheSettings.h"

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Interpreters/Cache/FileCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

#define LIST_OF_FILE_CACHE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, path, "", "Cache directory path", 0) \
    DECLARE(UInt64, max_size, 0, "Maximum cache size", 0) \
    DECLARE(UInt64, max_elements, FILECACHE_DEFAULT_MAX_ELEMENTS, "Maximum number of cache elements, e.g. file segments (limits number of files on filesystem)", 0) \
    DECLARE(UInt64, max_file_segment_size, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE, "Maximum size of a single file segment", 0) \
    DECLARE(UInt64, boundary_alignment, FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT, "File segment alignment", 0) \
    DECLARE(Bool, cache_on_write_operations, false, "Enables write-through cache (cache on INSERT and MERGE)", 0) \
    DECLARE(String, cache_policy, "LRU", "Cache eviction policy", 0) \
    DECLARE(Double, slru_size_ratio, 0.6, "SLRU cache policy size ratio of protected to probationary elements", 0) \
    DECLARE(UInt64, background_download_threads, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS, "Number of background download threads. Value 0 disables background download", 0) \
    DECLARE(UInt64, background_download_queue_size_limit, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT, "Size of background download queue. Value 0 disables background download", 0) \
    DECLARE(UInt64, background_download_max_file_segment_size, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD, "Maximum size which can be downloaded in background download", 0) \
    DECLARE(UInt64, load_metadata_threads, FILECACHE_DEFAULT_LOAD_METADATA_THREADS, "Number of threads to load cache metadata at server startup. Value 0 disables asynchronous loading of metadata", 0) \
    DECLARE(UInt64, load_metadata_asynchronously, false, "Enables asynchronous loading of metadata on server startup", 0) \
    DECLARE(Double, keep_free_space_size_ratio, FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO, "A ratio of free space which cache would try to uphold in the background", 0) \
    DECLARE(Double, keep_free_space_elements_ratio, FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO, "A ratio of free elements which cache would try to uphold in the background", 0) \
    DECLARE(UInt64, keep_free_space_remove_batch, FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH, "A remove batch size of cache elements made by background thread which upholds free space/elements ratio", 0) \
    DECLARE(Bool, enable_filesystem_query_cache_limit, false, "Enable limiting maximum size of cache which can be written within a query", 0) \
    DECLARE(Bool, enable_bypass_cache_with_threshold, false, "", 0) \
    DECLARE(UInt64, cache_hits_threshold, 0, "", 0) \
    DECLARE(Bool, bypass_cache_threshold, 0, "", 0) \
    DECLARE(Bool, write_cache_per_user_id_directory, false, "Private setting", 0)

DECLARE_SETTINGS_TRAITS(FileCacheSettingsTraits, LIST_OF_FILE_CACHE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(FileCacheSettingsTraits, LIST_OF_FILE_CACHE_SETTINGS)

struct FileCacheSettingsImpl : public BaseSettings<FileCacheSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    FileCacheSettings##TYPE NAME = &FileCacheSettingsImpl ::NAME;

namespace FileCacheSetting
{
LIST_OF_FILE_CACHE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
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

NamesAndTypesList FileCacheSettings::getSampleBlock()
{
    FileCacheSettingsImpl impl;
    NamesAndTypesList result;
    for (const auto & setting : impl.all())
    {
        const auto & setting_name = setting.getName();
        const std::string type_name = setting.getTypeName();
        DataTypePtr data_type;
        if (type_name == "UInt64")
            data_type = std::make_shared<DataTypeUInt64>();
        else if (type_name == "String")
            data_type = std::make_shared<DataTypeString>();
        else if (type_name == "Bool")
            data_type = std::make_shared<DataTypeUInt8>();
        else if (type_name == "Double")
            data_type = std::make_shared<DataTypeFloat64>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", type_name);

        result.emplace_back(setting_name, data_type);
    }
    result.emplace_back("is_initialized", std::make_shared<DataTypeUInt8>());
    return result;
}

void FileCacheSettings::dumpToSystemSettingsColumns(MutableColumnsAndConstraints & params, const FileCachePtr & cache) const
{
    MutableColumns & res_columns = params.res_columns;
    size_t i = 0;
    for (const auto & setting : impl->all())
        res_columns[i++]->insert(setting.getValue());
    res_columns[i++]->insert(cache->isInitialized());
}

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
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
}

void FileCacheSettings::loadFromCollection(const NamedCollection & collection)
{
    for (const auto & key : collection.getKeys())
    {
        impl->set(key, collection.get<String>(key));
    }
}

}
