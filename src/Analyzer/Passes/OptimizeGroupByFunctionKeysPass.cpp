#include <Analyzer/Passes/OptimizeGroupByFunctionKeysPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>

namespace DB
{

class OptimizeGroupByFunctionKeysVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>;

    OptimizeGroupByFunctionKeysVisitor(UsableGroupByKeys usable_keys_, ContextPtr context)
        : Base(std::move(context))
        , usable_keys(usable_keys_)
    {}

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (parent->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            return false;

        return !child->as<FunctionNode>();
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query = node->as<QueryNode>();
        if (!query || !canRemoveGroupByKeysThatAreFunctionsOfOtherKeys(*query))
            return;

        auto & group_by = query->getGroupBy().getNodes();
        removeKeysThatAreFunctionsOfOtherKeys(group_by, {}, usable_keys, getGroupingFunctionArguments(*query));
    }

private:
    const UsableGroupByKeys usable_keys;
};

void OptimizeGroupByFunctionKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByFunctionKeysVisitor visitor(usable_keys, std::move(context));
    visitor.visit(query_tree_node);
}

}
