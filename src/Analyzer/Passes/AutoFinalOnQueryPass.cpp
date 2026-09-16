#include <Analyzer/Passes/AutoFinalOnQueryPass.h>

#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool final;
}

namespace
{

class AutoFinalOnQueryPassVisitor : public InDepthQueryTreeVisitorWithContext<AutoFinalOnQueryPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<AutoFinalOnQueryPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::final])
            return;

        const auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto table_expressions = extractTableExpressions(query_node->getJoinTree());
        for (auto & table_expression : table_expressions)
            applyFinalIfNeeded(table_expression);
    }
private:
    static void applyFinalIfNeeded(QueryTreeNodePtr & node)
    {
        auto * table_node = node->as<TableNode>();
        auto * table_function_node = node->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            return;

        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        bool is_final_supported = storage && !storage->isRemote() && storage->supportsFinal();
        if (!is_final_supported)
            return;

        TableExpressionModifiers table_expression_modifiers_with_final(true /*has_final*/, {}, {});

        if (table_node)
        {
            if (table_node->hasTableExpressionModifiers())
                table_node->getTableExpressionModifiers()->setHasFinal(true);
            else
                table_node->setTableExpressionModifiers(table_expression_modifiers_with_final);
        }
        else if (table_function_node)
        {
            if (table_function_node->hasTableExpressionModifiers())
                table_function_node->getTableExpressionModifiers()->setHasFinal(true);
            else
                table_function_node->setTableExpressionModifiers(table_expression_modifiers_with_final);
        }
    }
};

}

void AutoFinalOnQueryPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto visitor = AutoFinalOnQueryPassVisitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
