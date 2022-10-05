#include "StorageSystemFilesystemCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

NamesAndTypesList StorageSystemFilesystemCache::getNamesAndTypes()
{
    return {
        {"cache_name", std::make_shared<DataTypeString>()},
        {"cache_base_path", std::make_shared<DataTypeString>()},
        {"cache_path", std::make_shared<DataTypeString>()},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>()},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"state", std::make_shared<DataTypeString>()},
        {"cache_hits", std::make_shared<DataTypeUInt64>()},
        {"references", std::make_shared<DataTypeUInt64>()},
        {"downloaded_size", std::make_shared<DataTypeUInt64>()},
        {"download_start_time", std::make_shared<DataTypeDateTime>()},
        {"download_end_time", std::make_shared<DataTypeDateTime>()},
        {"background_download_in_progress_range", std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())})},
        {"background_download_wait_ranges", std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeTuple>(
                    DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()}))},
        {"persistent", std::make_shared<DataTypeNumber<UInt8>>()}
    };
}

StorageSystemFilesystemCache::StorageSystemFilesystemCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemFilesystemCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto caches = FileCacheFactory::instance().getAll();

    for (const auto & [cache_name, cache_data] : caches)
    {
        const auto & cache = cache_data->cache;
        auto file_segments = cache->getSnapshot();

        for (const auto & file_segment : file_segments)
        {
            FileSegment::Stat stat = file_segment->getStat();

            res_columns[0]->insert(cache_name);
            res_columns[1]->insert(cache->getBasePath());
            res_columns[2]->insert(
                cache->getPathInLocalCache(file_segment->key(), file_segment->offset(), file_segment->isPersistent()));

            const auto & range = file_segment->range();
            res_columns[3]->insert(range.left);
            res_columns[4]->insert(range.right);
            res_columns[5]->insert(range.size());
            res_columns[6]->insert(FileSegment::stateToString(file_segment->state()));
            res_columns[7]->insert(file_segment->getHitsCount());
            res_columns[8]->insert(file_segment->getRefCount());
            res_columns[9]->insert(file_segment->getDownloadedSize());
            res_columns[10]->insert(stat.download_start_time);
            res_columns[11]->insert(stat.download_end_time);

            {
                std::unique_lock segment_lock(file_segment->mutex);
                auto & state = file_segment->background_download;
                if (state)
                {
                    auto currently_downloading = state->getCurrentlyDownloadingRange(segment_lock);
                    if (currently_downloading)
                        res_columns[12]->insert(Tuple{currently_downloading->left, currently_downloading->right});
                    else
                        res_columns[12]->insertDefault();

                    auto currently_waiting = state->getDownloadQueueRanges(segment_lock);
                    std::vector<Tuple> ranges;
                    for (const auto & waiting_range : currently_waiting)
                        ranges.emplace_back(waiting_range.left, waiting_range.right);
                    res_columns[13]->insert(Array(ranges.begin(), ranges.end()));
                }
                else
                {
                    res_columns[12]->insertDefault();
                    res_columns[13]->insertDefault();
                }
            }

            res_columns[14]->insert(file_segment->isPersistent());
        }
    }
}

}
