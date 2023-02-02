#include "AutoFinalOnQueryPass.h"

#include <Analyzer/TableNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Storages/IStorage.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

namespace DB
{

namespace
{
    class AutoFinalOnQueryPassVisitor : public InDepthQueryTreeVisitorWithContext<AutoFinalOnQueryPassVisitor>
    {
    public:
        using Base = InDepthQueryTreeVisitorWithContext<AutoFinalOnQueryPassVisitor>;
        using Base::Base;

        void visitImpl(QueryTreeNodePtr & node)
        {
            if (auto * table_node = node->as<TableNode>())
            {
                if (autoFinalOnQuery(*table_node, table_node->getStorage(), getContext()))
                {
                    auto modifier = TableExpressionModifiers(true, std::nullopt, std::nullopt);
                    table_node->setTableExpressionModifiers(modifier);
                }
            }
        }

    private:
        static bool autoFinalOnQuery(TableNode & table_node, StoragePtr storage, ContextPtr context)
        {
            bool is_auto_final_setting_on = context->getSettingsRef().final;
            bool is_final_supported = storage && storage->supportsFinal() && !storage->isRemote();
            bool is_query_already_final = table_node.hasTableExpressionModifiers() ? table_node.getTableExpressionModifiers().has_value() : false;

            return is_auto_final_setting_on && !is_query_already_final && is_final_supported;
        }

    };

}

String AutoFinalOnQueryPass::getName()
{
    return "AutoFinalOnQueryPass";
}

String AutoFinalOnQueryPass::getDescription()
{
    return "Automatically applies final modifier to queries if it is supported and if user level final setting is set.";
}

void AutoFinalOnQueryPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    auto visitor = AutoFinalOnQueryPassVisitor(std::move(context));

    visitor.visit(query_tree_node);
}

}
