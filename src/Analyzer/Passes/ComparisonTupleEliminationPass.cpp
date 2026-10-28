#include <Analyzer/Passes/ComparisonTupleEliminationPass.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>

#include <Common/assert_cast.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Utils.h>

#include <base/unit.h>

#include <algorithm>

namespace DB
{

namespace
{

/// Argument columns as `resolveFunction` folds with them: a constant or a known `__getScalar` argument has a column.
ColumnsWithTypeAndName getFoldingArgumentColumns(const FunctionNode & function_node, const ContextPtr & context)
{
    auto arguments = function_node.getArgumentColumns();
    const auto & argument_nodes = function_node.getArguments().getNodes();
    for (size_t i = 0; i < argument_nodes.size(); ++i)
    {
        if (!arguments[i].column)
            arguments[i].column = tryGetScalarConstantColumn(argument_nodes[i], context);
    }
    return arguments;
}

bool areAllConstant(const ColumnsWithTypeAndName & arguments)
{
    return std::ranges::all_of(arguments, [](const auto & argument) { return argument.column != nullptr; });
}

/// The constant `resolveFunction` would replace the function `node` with, given its argument columns, or nullptr.
ColumnPtr tryFoldFunction(const QueryTreeNodePtr & node, const ColumnsWithTypeAndName & arguments)
{
    const auto & function = node->as<const FunctionNode &>().getFunction();
    if (!function->isSuitableForConstantFolding())
        return nullptr;

    ColumnPtr column;
    if (areAllConstant(arguments))
        column = function->prepare(arguments)->execute(arguments, function->getResultType(), 1, /*dry_run=*/ true);
    else
        column = function->getConstantResultForNonConstArguments(arguments, function->getResultType());

    const auto * column_const = column ? typeid_cast<const ColumnConst *>(column.get()) : nullptr;
    if (!column_const || column_const->getDataColumn().isDummy() || hasAggregateFunctionNodes(node)
        || hasFunctionNode(node, "arrayJoin") || column->byteSize() >= 1_MiB)
        return nullptr;

    return column;
}

bool hasColumnNode(const QueryTreeNodePtr & node)
{
    if (node->getNodeType() == QueryTreeNodeType::COLUMN)
        return true;
    return std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasColumnNode(child); });
}

class ComparisonTupleEliminationPassVisitor : public InDepthQueryTreeVisitorWithContext<ComparisonTupleEliminationPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ComparisonTupleEliminationPassVisitor>;
    using Base::Base;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (parent->getNodeType() == QueryTreeNodeType::JOIN)
        {
            /// In JOIN ON section comparison of tuples works a bit differently.
            /// For example we can join on tuple(NULL) = tuple(NULL), join algorithms consider only NULLs on the top level.
            if (parent->as<const JoinNode &>().getJoinExpression().get() == child.get())
                return false;
        }
        return child->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node) const
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & comparison_function_name = function_node->getFunctionName();
        if (comparison_function_name != "equals" && comparison_function_name != "notEquals")
            return;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        const auto & lhs_argument = arguments[0];
        const auto & lhs_argument_result_type = lhs_argument->getResultType();
        if (!isTuple(lhs_argument_result_type))
            return;

        const auto & rhs_argument = arguments[1];
        const auto & rhs_argument_result_type = rhs_argument->getResultType();
        if (!isTuple(rhs_argument_result_type))
            return;

        if (function_node->getResultType()->equals(DataTypeNullable(std::make_shared<DataTypeNothing>())))
            /** The function `equals` can return Nullable(Nothing), e.g., in the case of (a, b) == (NULL, 1).
              * On the other hand, `AND` returns Nullable(UInt8), so we would need to convert types.
              * It's better to just skip this trivial case.
              */
            return;

        auto lhs_argument_node_type = lhs_argument->getNodeType();
        auto rhs_argument_node_type = rhs_argument->getNodeType();

        QueryTreeNodePtr candidate;

        if (lhs_argument_node_type == QueryTreeNodeType::FUNCTION && rhs_argument_node_type == QueryTreeNodeType::FUNCTION)
            candidate = tryOptimizeComparisonTupleFunctions(lhs_argument, rhs_argument, comparison_function_name);
        else if (lhs_argument_node_type == QueryTreeNodeType::FUNCTION && rhs_argument_node_type == QueryTreeNodeType::CONSTANT)
            candidate = tryOptimizeComparisonTupleFunctionAndConstant(lhs_argument, rhs_argument, comparison_function_name);
        else if (lhs_argument_node_type == QueryTreeNodeType::CONSTANT && rhs_argument_node_type == QueryTreeNodeType::FUNCTION)
            candidate = tryOptimizeComparisonTupleFunctionAndConstant(rhs_argument, lhs_argument, comparison_function_name);

        if (candidate != nullptr)
        {
            if (node->getResultType()->equals(*candidate->getResultType()))
            {
                node = candidate;
            }
            else if (node->getResultType()->equals(*makeNullableSafe(candidate->getResultType())))
            {
                /// The node's result type may be wrapped in Nullable due to group_by_use_nulls.
                /// Transfer the Nullable wrapper to the candidate so the optimization still applies.
                candidate->convertToNullable();
                node = candidate;
            }
            else
            {
                return;
            }

            /// A single-element tuple comparison is replaced by the comparison of its elements, which can be tuples too.
            enterImpl(node);
        }
    }

private:
    QueryTreeNodePtr tryOptimizeComparisonTupleFunctions(
        const QueryTreeNodePtr & lhs_function_node,
        const QueryTreeNodePtr & rhs_function_node,
        const std::string & comparison_function_name) const
    {
        const auto & lhs_function_node_typed = lhs_function_node->as<FunctionNode &>();
        if (lhs_function_node_typed.getFunctionName() != "tuple")
            return {};

        const auto & rhs_function_node_typed = rhs_function_node->as<FunctionNode &>();
        if (rhs_function_node_typed.getFunctionName() != "tuple")
            return {};

        const auto & lhs_tuple_function_arguments_nodes = lhs_function_node_typed.getArguments().getNodes();
        size_t lhs_tuple_function_arguments_nodes_size = lhs_tuple_function_arguments_nodes.size();

        const auto & rhs_tuple_function_arguments_nodes = rhs_function_node_typed.getArguments().getNodes();
        if (lhs_tuple_function_arguments_nodes_size != rhs_tuple_function_arguments_nodes.size())
            return {};

        if (lhs_tuple_function_arguments_nodes_size == 1)
        {
            return makeNonFoldingComparisonFunction(lhs_tuple_function_arguments_nodes[0], rhs_tuple_function_arguments_nodes[0], comparison_function_name);
        }

        QueryTreeNodes tuple_arguments_equals_functions;
        tuple_arguments_equals_functions.reserve(lhs_tuple_function_arguments_nodes_size);

        for (size_t i = 0; i < lhs_tuple_function_arguments_nodes_size; ++i)
        {
            auto equals_function = makeEqualsFunction(lhs_tuple_function_arguments_nodes[i], rhs_tuple_function_arguments_nodes[i]);
            tuple_arguments_equals_functions.push_back(std::move(equals_function));
        }

        return makeEquivalentTupleComparisonFunction(std::move(tuple_arguments_equals_functions), comparison_function_name);
    }

    QueryTreeNodePtr tryOptimizeComparisonTupleFunctionAndConstant(
        const QueryTreeNodePtr & function_node,
        const QueryTreeNodePtr & constant_node,
        const std::string & comparison_function_name) const
    {
        const auto & function_node_typed = function_node->as<FunctionNode &>();
        if (function_node_typed.getFunctionName() != "tuple")
            return {};

        const auto & constant_node_typed = constant_node->as<ConstantNode &>();
        auto constant_node_result_type = constant_node_typed.getResultType();
        const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(constant_node_result_type.get());
        if (!tuple_data_type)
            return {};

        const auto & function_arguments_nodes = function_node_typed.getArguments().getNodes();
        size_t function_arguments_nodes_size = function_arguments_nodes.size();
        const auto & tuple_data_type_elements = tuple_data_type->getElements();
        if (tuple_data_type_elements.size() != function_arguments_nodes_size)
            return {};

        /// A `Field` of a `Dynamic` value does not keep its member type, so the elements are taken from the column.
        const auto & constant_tuple_column = assert_cast<const ColumnTuple &>(constant_node_typed.getColumn()->getDataColumn());
        auto make_element_constant_node = [&](size_t i)
        {
            return std::make_shared<ConstantNode>(ColumnConst::create(constant_tuple_column.getColumnPtr(i), 1), tuple_data_type_elements[i]);
        };

        if (function_arguments_nodes_size == 1)
            return makeNonFoldingComparisonFunction(function_arguments_nodes[0], make_element_constant_node(0), comparison_function_name);

        QueryTreeNodes tuple_arguments_equals_functions;
        tuple_arguments_equals_functions.reserve(function_arguments_nodes_size);

        for (size_t i = 0; i < function_arguments_nodes_size; ++i)
        {
            auto equals_function = makeEqualsFunction(function_arguments_nodes[i], make_element_constant_node(i));
            tuple_arguments_equals_functions.push_back(std::move(equals_function));
        }

        return makeEquivalentTupleComparisonFunction(std::move(tuple_arguments_equals_functions), comparison_function_name);
    }

    QueryTreeNodePtr makeEquivalentTupleComparisonFunction(QueryTreeNodes tuple_arguments_equals_functions,
        const std::string & comparison_function_name) const
    {
        /// A remote server analyzes the query again and names a folded constant that depends on a column differently.
        ColumnsWithTypeAndName and_arguments;
        for (const auto & equals_function : tuple_arguments_equals_functions)
        {
            auto folding_result = tryFoldFunction(equals_function, getFoldingArgumentColumns(equals_function->as<const FunctionNode &>(), getContext()));
            if (folding_result && hasColumnNode(equals_function))
                return {};
            and_arguments.emplace_back(std::move(folding_result), equals_function->getResultType(), "");
        }

        auto result_function = std::make_shared<FunctionNode>("and");
        result_function->markAsOperator();
        result_function->getArguments().getNodes() = std::move(tuple_arguments_equals_functions);
        resolveOrdinaryFunctionNodeByName(*result_function, result_function->getFunctionName(), getContext());

        if (tryFoldFunction(result_function, and_arguments))
            return {};

        if (comparison_function_name == "notEquals")
        {
            auto not_function = std::make_shared<FunctionNode>("not");
            not_function->markAsOperator();
            not_function->getArguments().getNodes().push_back(std::move(result_function));
            resolveOrdinaryFunctionNodeByName(*not_function, not_function->getFunctionName(), getContext());
            result_function = std::move(not_function);
        }

        return result_function;
    }

    QueryTreeNodePtr makeEqualsFunction(QueryTreeNodePtr lhs_argument, QueryTreeNodePtr rhs_argument) const
    {
        return makeComparisonFunction(std::move(lhs_argument), std::move(rhs_argument), "equals");
    }

    QueryTreeNodePtr makeComparisonFunction(QueryTreeNodePtr lhs_argument,
        QueryTreeNodePtr rhs_argument,
        const std::string & comparison_function_name) const
    {
        auto comparison_function = std::make_shared<FunctionNode>(comparison_function_name);
        comparison_function->getArguments().getNodes().push_back(std::move(lhs_argument));
        comparison_function->getArguments().getNodes().push_back(std::move(rhs_argument));
        comparison_function->markAsOperator();

        resolveOrdinaryFunctionNodeByName(*comparison_function, comparison_function->getFunctionName(), getContext());

        return comparison_function;
    }

    QueryTreeNodePtr makeNonFoldingComparisonFunction(QueryTreeNodePtr lhs_argument, QueryTreeNodePtr rhs_argument,
        const std::string & comparison_function_name) const
    {
        auto comparison_function = makeComparisonFunction(std::move(lhs_argument), std::move(rhs_argument), comparison_function_name);

        /// A remote server analyzes the query again and names the constant it folds this comparison into differently.
        if (tryFoldFunction(comparison_function, getFoldingArgumentColumns(comparison_function->as<const FunctionNode &>(), getContext())))
            return {};

        return comparison_function;
    }
};

}

void ComparisonTupleEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ComparisonTupleEliminationPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
