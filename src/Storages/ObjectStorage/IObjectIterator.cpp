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
    extern const SettingsBool use_query_condition_cache;
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
    const std::string & object_namespace_,
    const ContextPtr & context_)
    : WithContext(context_)
    , iterator(iterator_)
    , object_namespace(object_namespace_)
    , virtual_columns(virtual_columns_)
    , hive_partition_columns(hive_partition_columns_)
    , filter_actions(getExpressionActions(filter_, virtual_columns, context_))
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

            auto path = key;
            if (path.starts_with("/"))
                path = path.substr(1);
            path = std::filesystem::path(object_namespace) / path;

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
    if (format_filter_info && format_filter_info->condition_hash
        && context_->getSettingsRef()[Setting::use_query_condition_cache])
        query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
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
                auto matching_marks = query_condition_cache->read(
                    storage_id.uuid,
                    last_object_info->getFileName(),
                    *format_filter_info->condition_hash);
                if (matching_marks.has_value())
                {
                    has_cache_entry = true;
                    const auto & marks = *matching_marks;
                    for (size_t i = 0; i < marks.size(); ++i)
                        if (marks[i])
                            matching_row_groups.push_back(i);
                    if (matching_row_groups.empty())
                        continue;
                }
            }

            auto buffer = createReadBuffer(last_object_info->relative_path_with_metadata, object_storage, getContext(), log);
            size_t bucket_size = getContext()->getSettingsRef()[Setting::cluster_table_function_buckets_batch_size];
            auto file_bucket_infos = splitter->splitToBuckets(bucket_size, *buffer, format_settings);
            for (const auto & file_bucket : file_bucket_infos)
            {
                auto copy_object_info = *last_object_info;
                if (has_cache_entry)
                {
                    auto filtered = file_bucket->filterByMatchingRowGroups(matching_row_groups);
                    if (!filtered)
                        continue;
                    copy_object_info.file_bucket_info = std::move(filtered);
                }
                else
                {
                    copy_object_info.file_bucket_info = file_bucket;
                }
                pending_objects_info.push(std::make_shared<ObjectInfo>(copy_object_info));
            }
        }
    }

    auto result = pending_objects_info.front();
    pending_objects_info.pop();
    return result;
}

String ObjectInfo::getIdentifier() const
{
    String result = getPath();
    if (file_bucket_info)
        result += file_bucket_info->getIdentifier();
    return result;
}

}
