#include <Analyzer/Passes/HasToInPass.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_has_to_in;
}

namespace
{

class RewriteHasToInVisitor : public InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_has_to_in])
            return;

        auto * has_function_node = node->as<FunctionNode>();
        if (!has_function_node || has_function_node->getFunctionName() != "has")
            return;

        auto & has_function_arguments_nodes = has_function_node->getArguments().getNodes();
        if (has_function_arguments_nodes.size() != 2)
            return;

        /// Verify that the first argument is a constant array
        const auto * first_arg_constant = has_function_arguments_nodes[0]->as<ConstantNode>();
        if (!first_arg_constant)
            return;

        /// Verify that the first argument is actually an array type
        const auto & first_arg_type = has_function_arguments_nodes[0]->getResultType();
        if (!isArray(first_arg_type))
            return;

        /// The next few checks are to handle differences/quirks between has() and in()
        /// Verify that none of the values in the constant array are NULLs, because has() and in() treat NULLs differently
        const auto & element_type = (typeid_cast<const DataTypeArray *>(first_arg_type.get()))->getNestedType();
        WhichDataType data_type(element_type);
        if (data_type.isArray() || data_type.isTuple() || data_type.isObject() || data_type.isDynamic() || data_type.isNothing())
            return;

        const auto & array_field = first_arg_constant->getValue();
        const auto & array_value = array_field.safeGet<Array>();
        if (array_value.empty())
            return;
        for (const auto & element : array_value)
        {
            if (element.isNull())
                return;
        }

        const auto second_arg_type = has_function_arguments_nodes[1]->getResultType();
        WhichDataType expr_data_type(second_arg_type);
        if (second_arg_type->isNullable() ||
                expr_data_type.isArray() || expr_data_type.isTuple() || expr_data_type.isObject() || expr_data_type.isDynamic() || expr_data_type.isNothing())
            return;

        /// Rewrite has(const_array, elem) -> in(elem, const_array)
        std::swap(has_function_arguments_nodes[0], has_function_arguments_nodes[1]);
        resolveOrdinaryFunctionNodeByName(*has_function_node, "in", getContext());
    }
};

}

void RewriteHasToInPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteHasToInVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
