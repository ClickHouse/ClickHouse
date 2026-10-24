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
#include <Processors/Formats/IInputFormat.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/maskURIPassword.h>
#include <Common/logger_useful.h>

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
            auto path = formatObjectPath(*configuration, key, /*include_connection_info=*/false);

            /// `_path` exposes the metadata URI, while `_file` comes from the resolved key, which can differ
            /// from the URI suffix (e.g. a query part that is not part of the key).
            std::optional<std::vector<std::string>> file_names;
            if (auto metadata_path = object->getPathInDataLakeMetadata())
            {
                path = *metadata_path;
                file_names.emplace(std::vector<std::string>{object->getFileName()});
            }

            VirtualColumnUtils::filterByPathOrFile(
                keys, std::vector<std::string>{path}, filter_actions,
                virtual_columns, hive_partition_columns, getContext(),
                /*format_settings=*/std::nullopt, file_names ? &*file_names : nullptr);

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
    const StorageID & storage_id_,
    FormatFilterInfoPtr format_filter_info_)
    : WithContext(context_)
    , iterator(iterator_)
    , format(format_)
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
    while (pending_objects_info.empty())
    {
        auto last_object_info = iterator->next(id);
        if (!last_object_info)
            return {};

        auto splitter = FormatFactory::instance().getSplitter(format);
        if (splitter)
        {
            std::vector<size_t> matching_row_groups;
            bool has_cache_entry = false;
            if (query_condition_cache)
            {
                const auto query_condition_cache_key = last_object_info->getQueryConditionCacheIdentifier();
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
                        continue;
                    }
                }
            }

            auto storage_to_use = last_object_info->getResolvedStorage(object_storage);
            auto buffer = createReadBuffer(last_object_info->relative_path_with_metadata, storage_to_use, getContext(), log);
            size_t bucket_size = getContext()->getSettingsRef()[Setting::cluster_table_function_buckets_batch_size];
            auto file_bucket_infos = splitter->splitToBuckets(bucket_size, *buffer, format_settings);
            for (const auto & file_bucket : file_bucket_infos)
            {
                auto copy_object_info = last_object_info->clone();
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
                pending_objects_info.push(std::move(copy_object_info));
            }
        }
    }

    auto result = pending_objects_info.front();
    pending_objects_info.pop();
    return result;
}

String ObjectInfo::getIdentifier(bool include_file_bucket_info) const
{
    return getArchiveEntryKey(getIdentifierForPath(getPath(), include_file_bucket_info), archive_entry_index);
}

String ObjectInfo::getQueryConditionCacheIdentifier() const
{
    const auto metadata_path = getPathInDataLakeMetadata();
    if (!metadata_path)
        return getIdentifier(/*include_file_bucket_info=*/ false);

    String identifier = getIdentifierForPath(*metadata_path, /*include_file_bucket_info=*/ false);
    String redacted_path = metadata_path->substr(0, metadata_path->find_first_of("?#"));
    if (!maskURIUserinfo(redacted_path) && redacted_path.size() == metadata_path->size())
        return identifier;
    return getIdentifierForPath(redacted_path, /*include_file_bucket_info=*/ false) + "#path=" + sipHash128String(identifier);
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
