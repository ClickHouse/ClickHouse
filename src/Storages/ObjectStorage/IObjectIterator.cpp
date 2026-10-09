#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>

namespace ProfileEvents
{
    extern const Event ObjectStorageListedObjects;
    extern const Event ObjectStoragePredicateFilteredObjects;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 cluster_table_function_buckets_batch_size;
    extern const SettingsMaxThreads max_threads;
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
        return std::make_shared<ExpressionActions>(std::move(*filter));
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
    const String & format_,
    ObjectStoragePtr object_storage_,
    const ContextPtr & context_,
    size_t min_buckets_per_file_,
    const StorageID & storage_id_,
    FormatFilterInfoPtr format_filter_info_)
    : WithContext(context_)
    , iterator(iterator_)
    , format(format_)
    , object_storage(object_storage_)
    , format_settings(getFormatSettings(context_))
    , storage_id(storage_id_)
    , format_filter_info(std::move(format_filter_info_))
    , bucket_size(context_->getSettingsRef()[Setting::cluster_table_function_buckets_batch_size])
    , min_buckets_per_file(std::max<size_t>(1, min_buckets_per_file_))
    , max_in_flight(std::clamp<size_t>(context_->getSettingsRef()[Setting::max_threads], 1, 32))
    , runner(threadPoolCallbackRunnerUnsafe<std::vector<ObjectInfoPtr>>(getIOThreadPool().get(), ThreadName::BUCKET_SPLITTER))
{
    if (format_filter_info && format_filter_info->condition_hash)
        query_condition_cache = context_->getQueryConditionCache();
}

ObjectIteratorSplitByBuckets::~ObjectIteratorSplitByBuckets()
{
    std::lock_guard lock(mutex);
    for (auto & future : in_flight)
        future.wait();
}

std::vector<ObjectInfoPtr> ObjectIteratorSplitByBuckets::split(const ObjectInfoPtr & object_info) const
{
    auto splitter = FormatFactory::instance().getSplitter(object_info->getFileFormat().value_or(format));
    if (!splitter)
        return {object_info};

    std::vector<size_t> matching_row_groups;
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
            if (matching_row_groups.empty())
                return {};
        }
    }

    std::optional<size_t> file_size = object_info->getFileSizeHint();
    if (!file_size.has_value())
    {
        if (auto metadata = object_info->getObjectMetadata(); metadata && metadata->is_fetched && metadata->is_size_known)
            file_size = metadata->size_bytes;
    }

    std::vector<FileBucketInfoPtr> buckets;
    if (file_size.has_value())
        buckets = splitter->splitByByteRanges(bucket_size, min_buckets_per_file, *file_size, object_info->getSplitOffsetsHint());

    if (buckets.empty())
    {
        auto buffer = createReadBuffer(object_info->relative_path_with_metadata, object_storage, getContext(), log);
        buckets = splitter->splitToBuckets(bucket_size, min_buckets_per_file, *buffer, format_settings);
    }

    if (buckets.size() <= 1)
    {
        if (!has_cache_entry)
            return {object_info};

        auto file_bucket_info = FormatFactory::instance().getFileBucketInfo(object_info->getFileFormat().value_or(format));
        auto filtered = file_bucket_info->filterByMatchingRowGroups(matching_row_groups);
        if (!filtered)
            return {};
        auto copy = object_info->clone();
        copy->file_bucket_info = std::move(filtered);
        return {copy};
    }

    std::vector<ObjectInfoPtr> result;
    result.reserve(buckets.size());
    for (const auto & bucket : buckets)
    {
        auto copy = object_info->clone();
        if (has_cache_entry)
        {
            auto filtered = bucket->filterByMatchingRowGroups(matching_row_groups);
            if (!filtered)
                continue;
            copy->file_bucket_info = std::move(filtered);
        }
        else
        {
            copy->file_bucket_info = bucket;
        }
        result.push_back(std::move(copy));
    }
    return result;
}

void ObjectIteratorSplitByBuckets::scheduleSplits(size_t id)
{
    while (!iterator_exhausted && in_flight.size() < max_in_flight)
    {
        auto object_info = iterator->next(id);
        if (!object_info)
        {
            iterator_exhausted = true;
            break;
        }
        in_flight.push_back(runner([this, object_info] { return split(object_info); }, Priority{}));
    }
}

ObjectInfoPtr ObjectIteratorSplitByBuckets::next(size_t id)
{
    std::lock_guard lock(mutex);
    while (pending_objects_info.empty())
    {
        scheduleSplits(id);
        if (in_flight.empty())
            return {};

        auto objects = in_flight.front().get();
        in_flight.pop_front();
        for (auto & object : objects)
            pending_objects_info.push_back(std::move(object));
    }

    auto result = std::move(pending_objects_info.front());
    pending_objects_info.pop_front();
    return result;
}

String ObjectInfo::getIdentifier(bool include_file_bucket_info) const
{
    return getIdentifierForPath(getPath(), include_file_bucket_info);
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
