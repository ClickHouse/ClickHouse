#include <Analyzer/Passes/ConstMapElementToShardPass.h>

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

class ConstMapElementToShardVisitor : public InDepthQueryTreeVisitorWithContext<ConstMapElementToShardVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConstMapElementToShardVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node) const
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
        if (!type_map || type_map->getNumShards() == 1)
            return;

        auto * const_node = function_arguments_nodes[1]->as<ConstantNode>();
        if (!const_node)
            return;

        UInt32 key_hash = getWeakHash(const_node->getValue(), const_node->getResultType());
        UInt32 shard_num = key_hash % type_map->getNumShards();

        String subcolumn_name = ".shard" + toString(shard_num);
        column_node->setColumnName(column_node->getColumnName() + subcolumn_name);
    }
};

}

void ConstMapElementToShardPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    ConstMapElementToShardVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
