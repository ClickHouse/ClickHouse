#include <Analyzer/Passes/MapElementConstKeyToShardPass.h>

#include <Common/WeakHash.h>
#include <DataTypes/DataTypeMap.h>

#include <Storages/IStorage.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>

namespace DB
{

namespace
{

UInt32 getWeakHash(const Field & field, const DataTypePtr & type)
{
    auto tmp = type->createColumn();
    tmp->insert(field);

    WeakHash32 hash(1);
    tmp->updateWeakHash32(hash);
    return hash.getData()[0];
}

class MapElementConstKeyToShardVisitor : public InDepthQueryTreeVisitorWithContext<MapElementConstKeyToShardVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MapElementConstKeyToShardVisitor>;
    using Base::Base;

    static void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "arrayElement")
            return;

        auto & function_arguments_nodes = function_node->getArguments().getNodes();
        if (function_arguments_nodes.size() != 2)
            return;

        auto * column_node = function_arguments_nodes[0]->as<ColumnNode>();
        if (!column_node)
            return;

        auto * table_node = column_node->getColumnSource()->as<TableNode>();
        if (!table_node)
            return;

        const auto & storage = table_node->getStorage();
        if (!storage->supportsSubcolumns())
            return;

        const auto * type_map = typeid_cast<const DataTypeMap *>(column_node->getColumnType().get());
        if (!type_map)
            return;

        auto * const_node = function_arguments_nodes[1]->as<ConstantNode>();
        if (!const_node)
            return;

        UInt32 key_hash = getWeakHash(const_node->getValue(), const_node->getResultType());

        auto subcolumn_name = ".shard_" + toString(key_hash);
        auto new_type = std::make_shared<DataTypeMap>(type_map->getNestedType());

        column_node->setColumnName(column_node->getColumnName() + subcolumn_name);
        column_node->setColumnType(std::move(new_type));
    }
};

}

void MapElementConstKeyToShardPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    MapElementConstKeyToShardVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
