#include <Analyzer/Passes/ConvertEmptyStringComparisonToFunctionPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_empty_string_comparisons;
}

namespace
{

class ConvertEmptyStringComparisonToFunctionVisitor
    : public InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_empty_string_comparisons])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const String & func_name = function_node->getFunctionName();
        if (func_name != "equals" && func_name != "notEquals")
            return;

        const auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        // Identify which argument is the empty string literal
        int const_idx = -1;

        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * constant_node = args[i]->as<ConstantNode>())
            {
                if (isStringOrFixedString(constant_node->getResultType()))
                {
                    const Field & val = constant_node->getValue();
                    if (val.getType() == Field::Types::String && val.safeGet<String>().empty())
                    {
                        const_idx = static_cast<int>(i);
                        break;
                    }
                }
            }
        }

        if (const_idx == -1)
            return;

        size_t expr_idx = 1 - const_idx;
        const auto & expr_node = args[expr_idx];

        const auto expr_type = expr_node->getResultType();
        if (!expr_type || !isStringOrFixedString(expr_type))
            return;

        const String replacement_func = (func_name == "equals") ? "empty" : "notEmpty";

        auto replacement_node = std::make_shared<FunctionNode>(replacement_func);
        replacement_node->getArguments().getNodes().push_back(expr_node);

        resolveOrdinaryFunctionNodeByName(*replacement_node, replacement_func, getContext());

        node = std::move(replacement_node);
    }
};

}

void ConvertEmptyStringComparisonToFunctionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertEmptyStringComparisonToFunctionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
