#include "StorageSystemFilesystemCacheSettings.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

ColumnsDescription StorageSystemFilesystemCacheSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"cache_name", std::make_shared<DataTypeString>(), "Name of the cache object"},
        {"path", std::make_shared<DataTypeString>(), "Cache directory"},
        {"max_size", std::make_shared<DataTypeUInt64>(), "Cache size limit by the number of bytes"},
        {"max_elements", std::make_shared<DataTypeUInt64>(), "Cache size limit by the number of elements"},
        {"is_initialized", std::make_shared<DataTypeUInt8>(), "Whether the cache is initialized and ready to be used"},
        {"current_size", std::make_shared<DataTypeUInt64>(), "Current cache size by the number of bytes"},
        {"current_elements", std::make_shared<DataTypeUInt64>(), "Current cache size by the number of elements"},
        {"max_file_segment_size", std::make_shared<DataTypeUInt64>(), "Maximum allowed file segment size"},
        {"boundary_alignment", std::make_shared<DataTypeUInt64>(), "Boundary alignment of file segments"},
        {"cache_on_write_operations", std::make_shared<DataTypeUInt8>(), "Write-through cache enablemenet setting"},
        {"cache_hits_threshold", std::make_shared<DataTypeUInt8>(), "Cache hits threshold enablemenet setting"},
        {"background_download_threads", std::make_shared<DataTypeUInt64>(), "Number of background download threads"},
        {"background_download_queue_size_limit", std::make_shared<DataTypeUInt64>(), "Queue size limit for background download"},
        {"load_metadata_threads", std::make_shared<DataTypeUInt64>(), "Number of load metadata threads"},
        {"enable_bypass_cache_threshold", std::make_shared<DataTypeUInt64>(), "Bypass cache threshold limit enablement setting"},
    };
}

StorageSystemFilesystemCacheSettings::StorageSystemFilesystemCacheSettings(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemFilesystemCacheSettings::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    context->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

    auto caches = FileCacheFactory::instance().getAll();

    for (const auto & [cache_name, cache_data] : caches)
    {
        const auto & settings = cache_data->getSettings();
        const auto & cache = cache_data->cache;

        size_t i = 0;
        res_columns[i++]->insert(cache_name);
        res_columns[i++]->insert(settings.base_path);
        res_columns[i++]->insert(settings.max_size);
        res_columns[i++]->insert(settings.max_elements);
        res_columns[i++]->insert(cache->isInitialized());
        res_columns[i++]->insert(cache->getUsedCacheSize());
        res_columns[i++]->insert(cache->getFileSegmentsNum());
        res_columns[i++]->insert(settings.max_file_segment_size);
        res_columns[i++]->insert(settings.boundary_alignment);
        res_columns[i++]->insert(settings.cache_on_write_operations);
        res_columns[i++]->insert(settings.cache_hits_threshold);
        res_columns[i++]->insert(settings.background_download_threads);
        res_columns[i++]->insert(settings.background_download_queue_size_limit);
        res_columns[i++]->insert(settings.load_metadata_threads);
        res_columns[i++]->insert(settings.enable_bypass_cache_with_threshold);
    }
}

}
