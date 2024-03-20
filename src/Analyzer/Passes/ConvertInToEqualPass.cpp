#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/ConvertInToEqualPass.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

class ConvertInToEqualPassVisitor : public InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>;
    using Base::Base;

    FunctionOverloadResolverPtr createInternalFunctionEqualOverloadResolver()
    {
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(getContext()->getSettings().decimal_check_overflow));
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_in_to_equal)
            return;
        auto * func_node = node->as<FunctionNode>();
        if (!func_node || func_node->getFunctionName() != "in" || func_node->getArguments().getNodes().size() != 2)
            return ;
        auto args = func_node->getArguments().getNodes();
        auto * column_node = args[0]->as<ColumnNode>();
        auto * constant_node = args[1]->as<ConstantNode>();
        if (!column_node || !constant_node)
            return ;
        if (constant_node->getValue().getType() == Field::Types::Which::Tuple)
            return;

        auto equal_resolver = createInternalFunctionEqualOverloadResolver();
        auto equal = std::make_shared<FunctionNode>("equals");
        QueryTreeNodes arguments{column_node->clone(), constant_node->clone()};
        equal->getArguments().getNodes() = std::move(arguments);
        equal->resolveAsFunction(equal_resolver);
        node = equal;
    }
};

void ConvertInToEqualPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertInToEqualPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
