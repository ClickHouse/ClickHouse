#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/Archives/ArchiveEntryPositions.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

namespace ProfileEvents
{
    extern const Event ObjectStorageListedObjects;
    extern const Event ObjectStoragePredicateFilteredObjects;
    extern const Event ObjectStorageQueryConditionCacheSkippedFiles;
    extern const Event ObjectStorageQueryConditionCacheSkippedRowGroups;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 cluster_table_function_buckets_batch_size;
    extern const SettingsBool cluster_table_function_split_by_split_offsets;
}

static ExpressionActionsPtr getExpressionActions(
    const DB::ActionsDAG & filter_,
    const NamesAndTypesList & virtual_columns_,
    const ContextPtr & context_)
{
    auto filter = VirtualColumnUtils::createPathAndFileFilterDAG(filter_.getOutputs().at(0), virtual_columns_, context_);
    if (filter.has_value())
    {
        VirtualColumnUtils::buildSetsForDAG(*filter, context_);
        return std::make_shared<ExpressionActions>(std::move(*filter), ExpressionActionsSettings(context_));
    }
    return nullptr;
}

ObjectIteratorWithPathAndFileFilter::ObjectIteratorWithPathAndFileFilter(
    ObjectIterator iterator_,
    const DB::ActionsDAG & filter_,
    const NamesAndTypesList & virtual_columns_,
    const NamesAndTypesList & hive_partition_columns_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ContextPtr & context_,
    std::function<void(FileProgress)> file_progress_callback_)
    : WithContext(context_)
    , iterator(iterator_)
    , configuration(std::move(configuration_))
    , virtual_columns(virtual_columns_)
    , hive_partition_columns(hive_partition_columns_)
    , filter_actions(getExpressionActions(filter_, virtual_columns, context_))
    , file_progress_callback(std::move(file_progress_callback_))
{
}

ObjectInfoPtr ObjectIteratorWithPathAndFileFilter::next(size_t id)
{
    while (true)
    {
        auto object = iterator->next(id);
        if (!object)
            break;

        if (emit_profile_events)
            ProfileEvents::increment(ProfileEvents::ObjectStorageListedObjects);

        if (filter_actions)
        {
            const auto key = object->getPath();
            std::vector<std::string> keys({key});

            /// Must be the formatter the `_path` column is produced with: this filter is
            /// evaluated against that column's values.
            const auto path = formatObjectPath(*configuration, key, /*include_connection_info=*/false);

            VirtualColumnUtils::filterByPathOrFile(
                keys, std::vector<std::string>{path}, filter_actions,
                virtual_columns, hive_partition_columns, getContext());

            if (keys.empty())
            {
                if (emit_profile_events)
                    ProfileEvents::increment(ProfileEvents::ObjectStoragePredicateFilteredObjects);
                LOG_TRACE(log, "Filtered out object: {}", object->getPath());
                continue;
            }
        }

        if (file_progress_callback)
        {
            if (auto metadata = object->getObjectMetadata())
                file_progress_callback(FileProgress(0, metadata->size_bytes));
            else if (auto hint = object->getFileSizeHint())
                file_progress_callback(FileProgress(0, *hint));
        }

        return object;
    }
    return {};
}

std::string ObjectInfo::getPathOrPathToArchiveIfArchive() const
{
    if (isArchive())
        return getPathToArchive();
    return getPath();
}


ObjectIteratorSplitByBuckets::ObjectIteratorSplitByBuckets(
    ObjectIterator iterator_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const ContextPtr & context_,
    const StorageID & storage_id_,
    FormatFilterInfoPtr format_filter_info_)
    : WithContext(context_)
    , iterator(iterator_)
    , configuration(std::move(configuration_))
    , object_storage(object_storage_)
    , format_settings(getFormatSettings(context_))
    , storage_id(storage_id_)
    , format_filter_info(std::move(format_filter_info_))
{
    if (format_filter_info && format_filter_info->condition_hash)
        query_condition_cache = context_->getQueryConditionCache();
}

ObjectInfoPtr ObjectIteratorSplitByBuckets::next(size_t id)
{
    while (true)
    {
        {
            std::unique_lock lock(mutex);
            objects_split_cv.wait(
                lock, [&] { return !pending_objects_info.empty() || !iterator_exhausted || objects_being_split == 0; });

            if (!pending_objects_info.empty())
            {
                auto result = std::move(pending_objects_info.front());
                pending_objects_info.pop();
                return result;
            }

            if (iterator_exhausted)
                return {};

            ++objects_being_split;
        }

        SCOPE_EXIT({
            std::lock_guard lock(mutex);
            --objects_being_split;
            objects_split_cv.notify_all();
        });

        auto object_info = iterator->next(id);
        auto buckets = object_info ? splitToBuckets(object_info) : ObjectInfos{};

        std::lock_guard lock(mutex);
        if (!object_info)
            iterator_exhausted = true;
        for (auto & bucket : buckets)
            pending_objects_info.push(std::move(bucket));
    }
}

ObjectInfos ObjectIteratorSplitByBuckets::splitToBuckets(const ObjectInfoPtr & object_info) const
{
    auto splitter = FormatFactory::instance().getSplitter(configuration->format);
    if (!splitter)
        return {};

    ObjectInfos result;

    RowGroupIndices matching_row_groups;
    bool has_cache_entry = false;
    if (query_condition_cache)
    {
        const auto query_condition_cache_key = object_info->getIdentifier(/*include_file_bucket_info=*/ false);
        auto matching_marks = query_condition_cache->read(
            storage_id.uuid,
            query_condition_cache_key,
            *format_filter_info->condition_hash);
        if (matching_marks.has_value())
        {
            has_cache_entry = true;
            const auto & marks = *matching_marks;
            for (size_t i = 0; i < marks.size(); ++i)
                if (marks[i])
                    matching_row_groups.push_back(i);
            ProfileEvents::increment(
                ProfileEvents::ObjectStorageQueryConditionCacheSkippedRowGroups, marks.size() - matching_row_groups.size());
            if (matching_row_groups.empty())
            {
                ProfileEvents::increment(ProfileEvents::ObjectStorageQueryConditionCacheSkippedFiles);
                return {};
            }
        }
    }

    const auto & settings = getContext()->getSettingsRef();
    size_t bucket_size = settings[Setting::cluster_table_function_buckets_batch_size];
    VectorWithMemoryTracking<FileBucketInfoPtr> file_bucket_infos;
    if (settings[Setting::cluster_table_function_split_by_split_offsets] && !has_cache_entry)
    {
        if (auto file_size = object_info->getFileSizeHint())
            file_bucket_infos = splitter->splitByByteRanges(bucket_size, *file_size, object_info->getSplitOffsetsHint());
    }
    if (file_bucket_infos.empty())
    {
        auto & object = object_info->relative_path_with_metadata;
        if (!object.metadata)
            object.metadata = StorageObjectStorageSource::tryMakeObjectMetadataFromTableRecords(*configuration, *object_info, storage_id);
        auto buffer = createReadBuffer(object, object_storage, getContext(), log);
        file_bucket_infos = splitter->splitToBuckets(bucket_size, *buffer, object, format_settings, getContext());
    }
    for (const auto & file_bucket : file_bucket_infos)
    {
        auto copy_object_info = object_info->clone();
        if (has_cache_entry)
        {
            auto filtered = file_bucket->filterByMatchingRowGroups(matching_row_groups);
            if (!filtered)
                continue;
            copy_object_info->file_bucket_info = std::move(filtered);
        }
        else
        {
            copy_object_info->file_bucket_info = file_bucket;
        }
        result.push_back(std::move(copy_object_info));
    }
    return result;
}

String ObjectInfo::getIdentifier(bool include_file_bucket_info) const
{
    return getArchiveEntryKey(getIdentifierForPath(getPath(), include_file_bucket_info), archive_entry_index);
}

String ObjectInfo::getIdentifierForPath(const String & path, bool include_file_bucket_info) const
{
    String result = path;
    if (relative_path_with_metadata.read_source_index)
        result = std::to_string(*relative_path_with_metadata.read_source_index) + ":" + result;
    if (include_file_bucket_info && file_bucket_info)
        result += file_bucket_info->getIdentifier();
    return result;
}

}
