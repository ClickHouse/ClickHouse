#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/ExpressionActions.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

static ExpressionActionsPtr getExpressionActions(
    const DB::ActionsDAG & filter_,
    const NamesAndTypesList & virtual_columns_,
    const ContextPtr & context_)
{
    auto filter = VirtualColumnUtils::createPathAndFileFilterDAG(filter_.getOutputs().at(0), virtual_columns_);
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

        if (filter_actions)
        {
            const auto key = object->getPath();
            std::vector<std::string> keys({key});

            auto path = key;
            if (path.starts_with("/"))
                path = path.substr(1);
            path = std::filesystem::path(object_namespace) / path;

            VirtualColumnUtils::filterByPathOrFile(keys, std::vector<std::string>{path}, filter_actions, virtual_columns, hive_partition_columns, getContext());
            if (keys.empty())
                continue;
        }

        return object;
    }
    return {};
}

ObjectIteratorSplittedByRowGroups::ObjectIteratorSplittedByRowGroups(
    ObjectIterator iterator_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const ContextPtr & context_)
    : WithContext(context_)
    , iterator(iterator_)
    , configuration(configuration_)
    , object_storage(object_storage_)
{
}

ObjectInfoPtr ObjectIteratorSplittedByRowGroups::next(size_t id)
{
    if (!pending_objects_info.empty())
    {
        auto result = pending_objects_info.front();
        pending_objects_info.pop();
        return result;
    }
    auto last_object_info = iterator->next(id);
    if (!last_object_info)
        return {};
    
    auto buffer = object_storage->readObject(StoredObject(last_object_info->getPath()), getReadSettings());
    auto input_format = FormatFactory::instance().getInput(
        configuration->format, *buffer, {}, getContext(), {});

    auto chunks_count = input_format->getChunksCount();
    if (chunks_count)
    {
        for (size_t i = 0; i < *chunks_count; ++i)
        {
            auto copy_object_info = *last_object_info;
            copy_object_info.row_group_id = i;
            pending_objects_info.push(std::make_shared<ObjectInfo>(copy_object_info));   
        }
    }

    auto result = pending_objects_info.front();
    pending_objects_info.pop();
    return result;
}


}
