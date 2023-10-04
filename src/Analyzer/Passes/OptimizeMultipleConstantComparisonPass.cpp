#include <Analyzer/Passes/OptimizeMultipleConstantComparison.h>

#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Field.h>


namespace DB
{
static int second_pass = 0;

namespace
{

struct ComparisonNodes
{
    int cnt_greater_comps = 0;
    int cnt_less_comps = 0;
    int disable_this_node = 0;
    QueryTreeNodePtr lessNode;
    QueryTreeNodePtr greaterNode;

    ComparisonNodes(QueryNodePtr greater = nullptr, QueryNodePtr less = nullptr) : lessNode(less), greaterNode(greater)
    {
        cnt_less_comps = 0;
        cnt_greater_comps = 0;
        disable_this_node = 0;
    }

    ~ComparisonNodes()
    {
        cnt_less_comps = 0;
        cnt_greater_comps = 0;
        lessNode = nullptr;
        greaterNode = nullptr;
    }
};

static std::unordered_map<std::string, ComparisonNodes> first_comp_nodes;

class OptimizeMultipleConstantComparisonVisitor : public InDepthQueryTreeVisitor<OptimizeMultipleConstantComparisonVisitor>
{
public:
    explicit OptimizeMultipleConstantComparisonVisitor(ContextPtr context_) : context(std::move(context_)) { }

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
        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        const auto & lhs_argument = arguments[0];
        const auto & rhs_argument = arguments[1];

        if (comparison_function_name == "or")
        {
            auto * lhs_argument_function = lhs_argument->as<FunctionNode>();
            if (lhs_argument_function && isValidComparisonNode(lhs_argument_function, lhs_argument_function->getFunctionName()))
                disableComparisonNode(lhs_argument_function->getArguments().getNodes()[0]);

            auto * rhs_argument_function = rhs_argument->as<FunctionNode>();
            if (rhs_argument_function && isValidComparisonNode(rhs_argument_function, rhs_argument_function->getFunctionName()))
                disableComparisonNode(rhs_argument_function->getArguments().getNodes()[0]);
            return;
        }

        if (!isValidComparisonNode(function_node, comparison_function_name))
            return;

        if (comparison_function_name == "greater" || comparison_function_name == "greaterOrEquals")
        {
            if (second_pass)
                tryEleminateGreaterComparison(node, lhs_argument);
            else
                tryOptimizeGreaterComparison(node, lhs_argument, rhs_argument, comparison_function_name);
        }
        else if (comparison_function_name == "less" || comparison_function_name == "lessOrEquals")
        {
            if (second_pass)
                tryEleminateLessComparison(node, lhs_argument);
            else
                tryOptimizeLessComparison(node, lhs_argument, rhs_argument, comparison_function_name);
        }
    }

private:
    inline bool isValidComparisonNode(const FunctionNode * function_node, const std::string & comparison_function_name) const
    {
        if (comparison_function_name != "greater" && comparison_function_name != "greaterOrEquals" && comparison_function_name != "less"
            && comparison_function_name != "lessOrEquals")
            return false;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return false;

        const auto & lhs_argument = arguments[0];
        const auto & lhs_argument_result_type = lhs_argument->getResultType();
        auto lhs_argument_node_type = lhs_argument->getNodeType();
        if (lhs_argument_node_type != QueryTreeNodeType::COLUMN)
            return false;

        const auto & rhs_argument = arguments[1];
        const auto & rhs_argument_result_type = rhs_argument->getResultType();
        auto rhs_argument_node_type = rhs_argument->getNodeType();
        if (rhs_argument_node_type != QueryTreeNodeType::CONSTANT)
            return false;

        auto & rhs_const = rhs_argument->as<ConstantNode &>();
        const Field & value = rhs_const.getValue();
        if (!isInt64OrUInt64FieldType(value.getType()))
            return false;
        return true;
    }

    inline void disableComparisonNode(const QueryTreeNodePtr & node) const
    {
        const auto & node_typed = node->as<ColumnNode &>();
        ComparisonNodes & comp_nodes = first_comp_nodes[node_typed.getColumnName()];
        comp_nodes.disable_this_node = 1;
    }

    inline void tryEleminateGreaterComparison(QueryTreeNodePtr & node, const QueryTreeNodePtr & lhs_argument) const
    {
        const auto & lhs_argument_node_typed = lhs_argument->as<ColumnNode &>();
        ComparisonNodes & comp_nodes = first_comp_nodes[lhs_argument_node_typed.getColumnName()];
        if (comp_nodes.disable_this_node)
        {
            --comp_nodes.cnt_greater_comps;
            if (comp_nodes.cnt_greater_comps == 0 && comp_nodes.cnt_less_comps == 0)
                first_comp_nodes.erase(lhs_argument_node_typed.getColumnName());
            return;
        }

        if (comp_nodes.cnt_greater_comps > 1)
        {
            auto * function_node = node->as<FunctionNode>();
            node = function_node->getArguments().getNodes()[1];
        }

        --comp_nodes.cnt_greater_comps;
        if (comp_nodes.cnt_less_comps == 0 && comp_nodes.cnt_greater_comps == 0)
            first_comp_nodes.erase(lhs_argument_node_typed.getColumnName());
    }

    inline void tryEleminateLessComparison(QueryTreeNodePtr & node, const QueryTreeNodePtr & lhs_argument) const
    {
        const auto & lhs_argument_node_typed = lhs_argument->as<ColumnNode &>();
        ComparisonNodes & comp_nodes = first_comp_nodes[lhs_argument_node_typed.getColumnName()];
        if (comp_nodes.disable_this_node)
        {
            --comp_nodes.cnt_less_comps;
            if (comp_nodes.cnt_greater_comps == 0 && comp_nodes.cnt_less_comps == 0)
                first_comp_nodes.erase(lhs_argument_node_typed.getColumnName());
            return;
        }

        if (comp_nodes.cnt_less_comps > 1)
        {
            auto * function_node = node->as<FunctionNode>();
            node = function_node->getArguments().getNodes()[1];
        }

        --comp_nodes.cnt_less_comps;
        if (comp_nodes.cnt_less_comps == 0 && comp_nodes.cnt_greater_comps == 0)
            first_comp_nodes.erase(lhs_argument_node_typed.getColumnName());
    }

    inline void tryOptimizeGreaterComparison(
        QueryTreeNodePtr & node,
        const QueryTreeNodePtr & lhs_argument,
        const QueryTreeNodePtr & rhs_argument,
        const std::string & comparison_function_name) const
    {
        const auto & lhs_argument_node_typed = lhs_argument->as<ColumnNode &>();
        const auto & rhs_argument_node_typed = rhs_argument->as<ConstantNode &>();
        const Field & current_value = rhs_argument_node_typed.getValue();
        ComparisonNodes & comp_nodes = first_comp_nodes[lhs_argument_node_typed.getColumnName()];
        if (comp_nodes.disable_this_node)
        {
            ++comp_nodes.cnt_greater_comps;
            return;
        }

        if (!comp_nodes.cnt_greater_comps)
        {
            comp_nodes.greaterNode = node;
            comp_nodes.cnt_greater_comps = 1;
            return;
        }

        ++comp_nodes.cnt_greater_comps;
        auto * function_node = comp_nodes.greaterNode->as<FunctionNode>();
        auto & arguments = function_node->getArguments().getNodes();
        auto & rhs_const = arguments[1]->as<ConstantNode &>();
        const Field & max_value = rhs_const.getValue();
        if (current_value > max_value)
            arguments[1] = makeConstantNode(current_value);

        node = makeComparisonFunction(lhs_argument, arguments[1], comparison_function_name);
    }

    inline void tryOptimizeLessComparison(
        QueryTreeNodePtr & node,
        const QueryTreeNodePtr & lhs_argument,
        const QueryTreeNodePtr & rhs_argument,
        const std::string & comparison_function_name) const
    {
        const auto & lhs_argument_node_typed = lhs_argument->as<ColumnNode &>();
        const auto & rhs_argument_node_typed = rhs_argument->as<ConstantNode &>();
        const Field & current_value = rhs_argument_node_typed.getValue();
        ComparisonNodes & comp_nodes = first_comp_nodes[lhs_argument_node_typed.getColumnName()];
        if (comp_nodes.disable_this_node)
        {
            ++comp_nodes.cnt_less_comps;
            return;
        }

        if (!comp_nodes.cnt_less_comps)
        {
            comp_nodes.lessNode = node;
            comp_nodes.cnt_less_comps = 1;
            return;
        }

        ++comp_nodes.cnt_less_comps;
        auto * function_node = comp_nodes.lessNode->as<FunctionNode>();
        auto & arguments = function_node->getArguments().getNodes();
        auto & rhs_const = arguments[1]->as<ConstantNode &>();
        const Field & min_value = rhs_const.getValue();
        if (current_value < min_value)
            arguments[1] = makeConstantNode(current_value);

        node = makeComparisonFunction(lhs_argument, arguments[1], comparison_function_name);
    }

    QueryTreeNodePtr makeConstantNode(const Field & value) const { return std::make_shared<ConstantNode>(value); }

    QueryTreeNodePtr
    makeComparisonFunction(QueryTreeNodePtr lhs_argument, QueryTreeNodePtr rhs_argument, const std::string & comparison_function_name) const
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

void OptimizeMultipleConstantComparisonPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    OptimizeMultipleConstantComparisonVisitor visitor(std::move(context));
    second_pass = second_pass_;
    visitor.visit(query_tree_node);
}

}
