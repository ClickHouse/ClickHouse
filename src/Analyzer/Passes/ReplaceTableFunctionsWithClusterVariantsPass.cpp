#include <memory>
#include <Analyzer/Passes/ReplaceTableFunctionsWithClusterVariantsPass.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <Core/Settings.h>

#include <Common/logger_useful.h>
#include "Analyzer/ConstantNode.h"
#include "Analyzer/IQueryTreeNode.h"
#include "TableFunctions/TableFunctionFactory.h"

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
}

namespace
{

class ReplaceTableFunctionsWithClusterVariantsPassVisitor : public InDepthQueryTreeVisitorWithContext<ReplaceTableFunctionsWithClusterVariantsPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ReplaceTableFunctionsWithClusterVariantsPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node) const
    {

        // if (getSettings()[Setting::max_parallel_replicas] == 1)
        //     return;
        //
        // if (getSettings()[Setting::parallel_replicas_mode] != ParallelReplicasMode::READ_TASKS)
        //     return;
        //
        // if (!getSettings()[Setting::parallel_replicas_for_cluster_engines])
        //     return;

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSource();

        auto * table_function_node = column_source->as<TableFunctionNode>();
        if (!table_function_node)
            return;

        LOG_DEBUG(&Poco::Logger::get("uwu"), "Node type: {}, tree: {}", node->getNodeType(), node->dumpTree());

        LOG_DEBUG(&Poco::Logger::get("uwu"), "Table function node: {}", table_function_node->getTableFunctionName());

        if (table_function_node->getTableFunctionName() != "url")
           return;

        const auto & storage = table_function_node->getStorage();
        LOG_DEBUG(&Poco::Logger::get("uwu"), "Storage: {}, is_remote: {}", storage->getName(), storage->isRemote());

        auto function_node = std::make_shared<TableFunctionNode>("urlCluster");
        function_node->setAlias(table_function_node->getAlias());

        // function_node->getChildren().clear();
        // function_node->getChildren().push_back(table_function_node->getArgumentsNode());
        function_node->getChildren().at(0)->getChildren().push_back(std::make_shared<ConstantNode>("default"));
        function_node->getChildren().at(0)->getChildren().push_back(std::make_shared<ConstantNode>("https://ifconfig.me"));

        LOG_DEBUG(&Poco::Logger::get("uwu"), "Args node original: {}", table_function_node->getArguments().dumpTree());
        LOG_DEBUG(&Poco::Logger::get("uwu"), "Args node ours: {}", function_node->getArguments().dumpTree());

        auto function = TableFunctionFactory::instance().tryGet(function_node->getTableFunctionName(), getContext());

        auto skip_analysis_arguments_indexes = function->skipAnalysisForArguments(function_node, getContext());
        auto table_function_storage = getContext()->getQueryContext()->executeTableFunction(function_node->toAST(), function);
        function_node->resolve(std::move(function), std::move(table_function_storage), getContext(), {});

        // column_node->setColumnSource(function_node);
        // LOG_DEBUG(&Poco::Logger::get("uwu"), "Column source: {}", column_node->dumpTree());
        // node = std::make_shared<ColumnNode>(column_node);
        LOG_DEBUG(&Poco::Logger::get("uwu"), "Node type: {}, tree: {}", function_node->getNodeType(), function_node->dumpTree());
        column_node->setColumnSource(function_node);
        column_node->temp = function_node;

        LOG_DEBUG(&Poco::Logger::get("uwu"), "Column source: {}", column_node->dumpTree());

        //
        // const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        // if (!storage->isRemote())
        //     return;
        //
        // const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
        // if (!storage->isVirtualColumn(column.name, storage_snapshot->metadata))
        //     return;
        //
        // auto function_node = std::make_shared<FunctionNode>("shardNum");
        // auto function = FunctionFactory::instance().get(function_node->getFunctionName(), getContext());
        // function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
        // node = std::move(function_node);
    }
};

}

void ReplaceTableFunctionsWithClusterVariantsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ReplaceTableFunctionsWithClusterVariantsPassVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
