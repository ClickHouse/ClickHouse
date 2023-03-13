#include <Analyzer/Passes/ConvertQueryToCNFPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Passes/CNF.h>

namespace DB
{

namespace
{

bool isLogicalFunction(const FunctionNode & function_node)
{
    const std::string_view name = function_node.getFunctionName();
    return name == "and" || name == "or" || name == "not";
}

class ConvertQueryToCNFVisitor : public InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>;
    using Base::Base;

    bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType &)
    {
        if (!getSettings().convert_query_to_cnf)
            return false;

        auto * function_node = parent->as<FunctionNode>();
        return !function_node || !isLogicalFunction(*function_node);
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().convert_query_to_cnf)
            return;

        auto * function_node = node->as<FunctionNode>();

        if (!function_node || !isLogicalFunction(*function_node))
            return;

        const auto & context = getContext();
        auto cnf_form = Analyzer::CNF::tryBuildCNF(node, context);
        if (!cnf_form)
            return;

        cnf_form->pushNotIntoFunctions(context);
        std::cout << "CNF " << cnf_form->dump() << std::endl;
        auto new_node = cnf_form->toQueryTree(context);
        if (!new_node)
            return;

        node = std::move(new_node);
    }
};

}

void ConvertQueryToCnfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    ConvertQueryToCNFVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}


}
