#include <Analyzer/Passes/ComparisonTupleEliminationPass.h>

#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

class ComparisonTupleEliminationPassVisitor : public InDepthQueryTreeVisitor<ComparisonTupleEliminationPassVisitor>
{
public:
    explicit ComparisonTupleEliminationPassVisitor(ContextPtr context_)
        : context(std::move(context_))
    {}

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        return child->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void visitImpl(QueryTreeNodePtr & node) const
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

        auto lhs_argument_node_type = lhs_argument->getNodeType();
        auto rhs_argument_node_type = rhs_argument->getNodeType();

        if (lhs_argument_node_type == QueryTreeNodeType::FUNCTION && rhs_argument_node_type == QueryTreeNodeType::FUNCTION)
            tryOptimizeComparisonTupleFunctions(node, lhs_argument, rhs_argument, comparison_function_name);
        else if (lhs_argument_node_type == QueryTreeNodeType::FUNCTION && rhs_argument_node_type == QueryTreeNodeType::CONSTANT)
            tryOptimizeComparisonTupleFunctionAndConstant(node, lhs_argument, rhs_argument, comparison_function_name);
        else if (lhs_argument_node_type == QueryTreeNodeType::CONSTANT && rhs_argument_node_type == QueryTreeNodeType::FUNCTION)
            tryOptimizeComparisonTupleFunctionAndConstant(node, rhs_argument, lhs_argument, comparison_function_name);
    }

private:
    void tryOptimizeComparisonTupleFunctions(QueryTreeNodePtr & node,
        const QueryTreeNodePtr & lhs_function_node,
        const QueryTreeNodePtr & rhs_function_node,
        const std::string & comparison_function_name) const
    {
        const auto & lhs_function_node_typed = lhs_function_node->as<FunctionNode &>();
        if (lhs_function_node_typed.getFunctionName() != "tuple")
            return;

        const auto & rhs_function_node_typed = rhs_function_node->as<FunctionNode &>();
        if (rhs_function_node_typed.getFunctionName() != "tuple")
            return;

        const auto & lhs_tuple_function_arguments_nodes = lhs_function_node_typed.getArguments().getNodes();
        size_t lhs_tuple_function_arguments_nodes_size = lhs_tuple_function_arguments_nodes.size();

        const auto & rhs_tuple_function_arguments_nodes = rhs_function_node_typed.getArguments().getNodes();
        if (lhs_tuple_function_arguments_nodes_size != rhs_tuple_function_arguments_nodes.size())
            return;

        if (lhs_tuple_function_arguments_nodes_size == 1)
        {
            node = makeComparisonFunction(lhs_tuple_function_arguments_nodes[0], rhs_tuple_function_arguments_nodes[0], comparison_function_name);
            return;
        }

        QueryTreeNodes tuple_arguments_equals_functions;
        tuple_arguments_equals_functions.reserve(lhs_tuple_function_arguments_nodes_size);

        for (size_t i = 0; i < lhs_tuple_function_arguments_nodes_size; ++i)
        {
            auto equals_function = makeEqualsFunction(lhs_tuple_function_arguments_nodes[i], rhs_tuple_function_arguments_nodes[i]);
            tuple_arguments_equals_functions.push_back(std::move(equals_function));
        }

        node = makeEquivalentTupleComparisonFunction(std::move(tuple_arguments_equals_functions), comparison_function_name);
    }

    void tryOptimizeComparisonTupleFunctionAndConstant(QueryTreeNodePtr & node,
        const QueryTreeNodePtr & function_node,
        const QueryTreeNodePtr & constant_node,
        const std::string & comparison_function_name) const
    {
        const auto & function_node_typed = function_node->as<FunctionNode &>();
        if (function_node_typed.getFunctionName() != "tuple")
            return;

        auto & constant_node_typed = constant_node->as<ConstantNode &>();
        const auto & constant_node_value = constant_node_typed.getValue();
        if (constant_node_value.getType() != Field::Types::Which::Tuple)
            return;

        const auto & constant_tuple = constant_node_value.get<const Tuple &>();

        const auto & function_arguments_nodes = function_node_typed.getArguments().getNodes();
        size_t function_arguments_nodes_size = function_arguments_nodes.size();
        if (function_arguments_nodes_size != constant_tuple.size())
            return;

        auto constant_node_result_type = constant_node_typed.getResultType();
        const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(constant_node_result_type.get());
        if (!tuple_data_type)
            return;

        const auto & tuple_data_type_elements = tuple_data_type->getElements();
        if (tuple_data_type_elements.size() != function_arguments_nodes_size)
            return;

        if (function_arguments_nodes_size == 1)
        {
            auto comparison_argument_constant_value = std::make_shared<ConstantValue>(constant_tuple[0], tuple_data_type_elements[0]);
            auto comparison_argument_constant_node = std::make_shared<ConstantNode>(std::move(comparison_argument_constant_value));
            node = makeComparisonFunction(function_arguments_nodes[0], std::move(comparison_argument_constant_node), comparison_function_name);
            return;
        }

        QueryTreeNodes tuple_arguments_equals_functions;
        tuple_arguments_equals_functions.reserve(function_arguments_nodes_size);

        for (size_t i = 0; i < function_arguments_nodes_size; ++i)
        {
            auto equals_argument_constant_value = std::make_shared<ConstantValue>(constant_tuple[i], tuple_data_type_elements[i]);
            auto equals_argument_constant_node = std::make_shared<ConstantNode>(std::move(equals_argument_constant_value));
            auto equals_function = makeEqualsFunction(function_arguments_nodes[i], std::move(equals_argument_constant_node));
            tuple_arguments_equals_functions.push_back(std::move(equals_function));
        }

        node = makeEquivalentTupleComparisonFunction(std::move(tuple_arguments_equals_functions), comparison_function_name);
    }

    QueryTreeNodePtr makeEquivalentTupleComparisonFunction(QueryTreeNodes tuple_arguments_equals_functions,
        const std::string & comparison_function_name) const
    {
        auto result_function = std::make_shared<FunctionNode>("and");
        result_function->getArguments().getNodes() = std::move(tuple_arguments_equals_functions);
        resolveOrdinaryFunctionNode(*result_function, result_function->getFunctionName());

        if (comparison_function_name == "notEquals")
        {
            auto not_function = std::make_shared<FunctionNode>("not");
            not_function->getArguments().getNodes().push_back(std::move(result_function));
            resolveOrdinaryFunctionNode(*not_function, not_function->getFunctionName());
            result_function = std::move(not_function);
        }

        return result_function;
    }

    inline QueryTreeNodePtr makeEqualsFunction(QueryTreeNodePtr lhs_argument, QueryTreeNodePtr rhs_argument) const
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

        resolveOrdinaryFunctionNode(*comparison_function, comparison_function->getFunctionName());

        return comparison_function;
    }

    void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function = FunctionFactory::instance().get(function_name, context);
        function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
    }

    ContextPtr context;
};

}

void ComparisonTupleEliminationPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    ComparisonTupleEliminationPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
