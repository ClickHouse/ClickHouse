#include <Analyzer/Passes/RewriteAggregateFunctionWithIfPass.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Core/Settings.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_aggregate_function_with_if;
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
}

namespace
{

/// A branch of `if` runs only on the rows its condition selects when lazy execution picks it up.
/// A nested short-circuit mask is not assumed to protect the rows the outer condition excluded, so
/// the walk continues through it.
bool branchCanBeSkippedByShortCircuit(const QueryTreeNodePtr & branch, bool every_function_is_lazy)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(branch.get());

    while (!nodes_to_process.empty())
    {
        const auto * node = nodes_to_process.back();
        nodes_to_process.pop_back();

        /// A constant is already evaluated when the query tree is built, so it cannot throw per row.
        if (node->as<ConstantNode>())
            continue;

        /// A table expression or a subquery in an argument slot is evaluated in its own scope, as a
        /// set or a scalar, so lazy execution of this expression never reaches inside it.
        switch (node->getNodeType())
        {
            case QueryTreeNodeType::TABLE:
            case QueryTreeNodeType::TABLE_FUNCTION:
            case QueryTreeNodeType::QUERY:
            case QueryTreeNodeType::UNION:
                continue;
            default:
                break;
        }

        if (const auto * function_node = node->as<FunctionNode>())
        {
            if (auto function_base = function_node->getFunction())
            {
                if (every_function_is_lazy)
                    return true;

                /// getArgumentColumns describes each slot the way the resolver did, so the `in`
                /// right-hand side is reported as a set instead of being asked for a result type.
                const auto argument_columns = function_node->getArgumentColumns();
                DataTypesWithConstInfo argument_types;
                argument_types.reserve(argument_columns.size());
                for (const auto & argument_column : argument_columns)
                    argument_types.push_back({argument_column.type, argument_column.column != nullptr});

                if (function_base->isSuitableForShortCircuitArgumentsExecution(argument_types))
                    return true;
            }
        }

        for (const auto & child : node->getChildren())
            if (child)
                nodes_to_process.push_back(child.get());
    }

    return false;
}

class RewriteAggregateFunctionWithIfVisitor : public InDepthQueryTreeVisitorWithContext<RewriteAggregateFunctionWithIfVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteAggregateFunctionWithIfVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_aggregate_function_with_if])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction())
            return;

        auto lower_name = Poco::toLower(function_node->getFunctionName());
        if (lower_name.ends_with("if"))
            return;

        const auto & function_arguments_nodes = function_node->getArguments().getNodes();
        if (function_arguments_nodes.size() != 1)
            return;

        auto * if_node = function_arguments_nodes[0]->as<FunctionNode>();
        if (!if_node || if_node->getFunctionName() != "if")
            return;

        FunctionNodePtr replaced_node;

        auto if_arguments_nodes = if_node->getArguments().getNodes();
        auto * first_const_node = if_arguments_nodes[1]->as<ConstantNode>();
        auto * second_const_node = if_arguments_nodes[2]->as<ConstantNode>();

        const auto short_circuit = getSettings()[Setting::short_circuit_function_evaluation];
        if (short_circuit != ShortCircuitFunctionEvaluation::DISABLE && (second_const_node || first_const_node))
        {
            /// The -If combinator evaluates its argument for every row, so lifting a branch that lazy
            /// execution would have skipped makes a throwing expression run on the rows the condition
            /// excluded.
            const auto & lifted_branch = second_const_node ? if_arguments_nodes[1] : if_arguments_nodes[2];
            if (branchCanBeSkippedByShortCircuit(lifted_branch, short_circuit == ShortCircuitFunctionEvaluation::FORCE_ENABLE))
                return;
        }

        if (second_const_node)
        {
            const auto & second_const_value = second_const_node->getValue();
            if (second_const_value.isNull()
                || (lower_name == "sum" && isInt64OrUInt64FieldType(second_const_value.getType()) && second_const_value.safeGet<UInt64>() == 0
                    && !if_node->getResultType()->isNullable()))
            {
                /// avg(if(cond, a, null)) -> avgIf(a::ResultTypeIf, cond)
                /// avg(if(cond, nullable_a, null)) -> avgIf(nullable_a, cond)
                /// sum(if(cond, a, 0)) -> sumIf(a, cond)
                /// sum(if(cond, nullable_a, 0)) **is not** equivalent to sumIfOrNull(cond, nullable_a) as
                ///     it changes the output when no rows pass the condition (from 0 to NULL)

                QueryTreeNodes new_arguments{2};

                /// We need to preserve the output type from if(). Notice that the return type of count() is the same either way
                if (if_arguments_nodes[1]->getResultType()->getName() != if_node->getResultType()->getName() && lower_name != "count")
                    new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[1]), if_node->getResultType(), getContext());
                else
                    new_arguments[0] = std::move(if_arguments_nodes[1]);

                new_arguments[1] = std::move(if_arguments_nodes[0]);

                replaced_node = std::make_shared<FunctionNode>(function_node->getFunctionName() + "If");
                replaced_node->getArguments().getNodes() = std::move(new_arguments);
                replaced_node->getParameters().getNodes() = function_node->getParameters().getNodes();
                resolveAggregateFunctionNodeByName(*replaced_node, replaced_node->getFunctionName());
            }
        }
        else if (first_const_node)
        {
            const auto & first_const_value = first_const_node->getValue();
            if (first_const_value.isNull()
                || (lower_name == "sum" && isInt64OrUInt64FieldType(first_const_value.getType()) && first_const_value.safeGet<UInt64>() == 0
                    && !if_node->getResultType()->isNullable()))
            {
                /// avg(if(cond, null, a) -> avgIf(a::ResultTypeIf, !cond))
                /// sum(if(cond, 0, a) -> sumIf(a, !cond))
                /// sum(if(cond, 0, nullable_a) **is not** sumIf(a, !cond)) -> Same as above

                QueryTreeNodes new_arguments{2};

                /// We need to preserve the output type from if(). Notice that the return type of count() is the same either way
                if (if_arguments_nodes[2]->getResultType()->getName() != if_node->getResultType()->getName() && lower_name != "count")
                    new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[2]), if_node->getResultType(), getContext());
                else
                    new_arguments[0] = std::move(if_arguments_nodes[2]);

                auto not_function = std::make_shared<FunctionNode>("not");
                not_function->markAsOperator();
                auto & not_function_arguments = not_function->getArguments().getNodes();
                not_function_arguments.push_back(std::move(if_arguments_nodes[0]));
                not_function->resolveAsFunction(
                    FunctionFactory::instance().get("not", getContext())->build(not_function->getArgumentColumns()));
                new_arguments[1] = std::move(not_function);

                replaced_node = std::make_shared<FunctionNode>(function_node->getFunctionName() + "If");
                replaced_node->getArguments().getNodes() = std::move(new_arguments);
                replaced_node->getParameters().getNodes() = function_node->getParameters().getNodes();
                resolveAggregateFunctionNodeByName(*replaced_node, replaced_node->getFunctionName());
            }
        }

        if (!replaced_node)
            return;

        auto prev_type = function_node->getResultType();
        auto curr_type = replaced_node->getResultType();
        if (!prev_type->equals(*curr_type))
            return;

        /// Just in case, CAST compatible aggregate function states.
        if (WhichDataType(prev_type).isAggregateFunction() && !DataTypeAggregateFunction::strictEquals(prev_type, curr_type))
            node = createCastFunction(std::move(replaced_node), prev_type, getContext());
        else
            node = std::move(replaced_node);
    }
};

}


void RewriteAggregateFunctionWithIfPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteAggregateFunctionWithIfVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
