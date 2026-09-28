#include <Analyzer/Passes/ShardNumColumnToFunctionPass.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

namespace DB
{

namespace
{

class ShardNumColumnToFunctionVisitor : public InDepthQueryTreeVisitorWithContext<ShardNumColumnToFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ShardNumColumnToFunctionVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node) const
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        const auto & column = column_node->getColumn();
        if (column.name != "_shard_num")
            return;

        auto column_source = column_node->getColumnSource();

        auto * table_node = column_source->as<TableNode>();
        auto * table_function_node = column_source->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            return;

        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        if (!storage->isRemote())
            return;

        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
        if (!storage->isVirtualColumn(column.name, storage_snapshot->metadata))
            return;

        const auto column_type = column_node->getColumnType();
        const auto shard_num_column_numeric_type = column_type->isNullable() ? typeid_cast<const DataTypeNullable *>(column_type.get())->getNestedType() : column_type;

        auto shard_num_function_node = std::make_shared<FunctionNode>("shardNum");
        auto shard_num_function = FunctionFactory::instance().get(shard_num_function_node->getFunctionName(), getContext());
        shard_num_function_node->resolveAsFunction(shard_num_function->build(shard_num_function_node->getArgumentColumns()));
        const auto & function_result_type = shard_num_function_node->getResultType();

        if (!function_result_type->equals(*shard_num_column_numeric_type))
            return;

        node = std::move(shard_num_function_node);
        if (column_type->isNullable())
            node = createCastFunction(node, column_type, getContext());
    }
};

}

void ShardNumColumnToFunctionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ShardNumColumnToFunctionVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
