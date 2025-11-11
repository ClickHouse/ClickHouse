#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

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

std::string ObjectInfo::getPathOrPathToArchiveIfArchive() const
{
    if (isArchive())
        return getPathToArchive();
    return getPath();
}

}
