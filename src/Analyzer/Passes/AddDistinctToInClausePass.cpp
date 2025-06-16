#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Passes/AddDistinctToInClausePass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDistributed.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_add_distinct_to_in_subqueries;
}
namespace
{

class FindQueryNodeVisitor : public InDepthQueryTreeVisitor<FindQueryNodeVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<FindQueryNodeVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto * query_node = node->as<QueryNode>())
        {
            if(!query_node->isDistinct())
                query_node->setIsDistinct(true);
        }
    }
};

class AddDistinctToInClauseVisitor : public InDepthQueryTreeVisitorWithContext<AddDistinctToInClauseVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<AddDistinctToInClauseVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::enable_add_distinct_to_in_subqueries])
            return;
        if (auto * function_node = node->as<FunctionNode>())
        {
            if (isNameOfInFunction(function_node->getFunctionName()))
            {
                auto in_arguments = function_node->getArgumentsNode();
                FindQueryNodeVisitor find_query_node_visitor;
                find_query_node_visitor.visit(in_arguments);
            }
        }
    }

};
}

void AddDistinctToInClausePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    AddDistinctToInClauseVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);

}
}
