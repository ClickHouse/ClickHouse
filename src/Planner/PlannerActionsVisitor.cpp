#include <Planner/PlannerActionsVisitor.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/LambdaNode.h>

#include <DataTypes/DataTypeSet.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

class ActionsScopeNode
{
public:
    explicit ActionsScopeNode(ActionsDAGPtr actions_dag_, QueryTreeNodePtr scope_node_)
        : actions_dag(std::move(actions_dag_))
        , scope_node(std::move(scope_node_))
    {
        for (const auto & node : actions_dag->getNodes())
            node_name_to_node[node.result_name] = &node;
    }

    const QueryTreeNodePtr & getScopeNode() const
    {
        return scope_node;
    }

    [[maybe_unused]] bool containsNode(const std::string & node_name)
    {
        return node_name_to_node.find(node_name) != node_name_to_node.end();
    }

    [[maybe_unused]] const ActionsDAG::Node * tryGetNode(const std::string & node_name)
    {
        auto it = node_name_to_node.find(node_name);
        if (it == node_name_to_node.end())
            return {};

        return it->second;
    }

    const ActionsDAG::Node * getNodeOrThrow(const std::string & node_name)
    {
        auto it = node_name_to_node.find(node_name);
        if (it == node_name_to_node.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No node with name {}. There are only nodes {}",
                node_name,
                actions_dag->dumpNames());

        return it->second;
    }

    const ActionsDAG::Node * addInputColumnIfNecessary(const std::string & node_name, const DataTypePtr & column_type)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addInput(node_name, column_type);
        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addInputConstantColumnIfNecessary(const std::string & node_name, const ColumnWithTypeAndName & column)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addInput(column);
        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addConstantIfNecessary(const std::string & node_name, const ColumnWithTypeAndName & column)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addColumn(column);
        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addFunctionIfNecessary(const std::string & node_name, ActionsDAG::NodeRawConstPtrs children, FunctionOverloadResolverPtr function)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addFunction(function, children, node_name);
        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addArrayJoinIfNecessary(const std::string & node_name, const ActionsDAG::Node * child)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addArrayJoin(*child, node_name);
        node_name_to_node[node->result_name] = node;

        return node;
    }

private:
    std::unordered_map<std::string_view, const ActionsDAG::Node *> node_name_to_node;
    ActionsDAGPtr actions_dag;
    QueryTreeNodePtr scope_node;
};

class PlannerActionsVisitorImpl
{
public:
    PlannerActionsVisitorImpl(ActionsDAGPtr actions_dag, const PlannerContextPtr & planner_context_);

    ActionsDAG::NodeRawConstPtrs visit(QueryTreeNodePtr expression_node);

private:
    using NodeNameAndNodeMinLevel = std::pair<std::string, size_t>;

    NodeNameAndNodeMinLevel visitImpl(QueryTreeNodePtr node);

    NodeNameAndNodeMinLevel visitColumn(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitConstant(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitLambda(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel makeSetForInFunction(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitFunction(const QueryTreeNodePtr & node);

    std::vector<ActionsScopeNode> actions_stack;
    std::unordered_map<const IQueryTreeNode *, std::string> node_to_node_name;
    const PlannerContextPtr planner_context;
};

PlannerActionsVisitorImpl::PlannerActionsVisitorImpl(ActionsDAGPtr actions_dag, const PlannerContextPtr & planner_context_)
    : planner_context(planner_context_)
{
    actions_stack.emplace_back(std::move(actions_dag), nullptr);
}

ActionsDAG::NodeRawConstPtrs PlannerActionsVisitorImpl::visit(QueryTreeNodePtr expression_node)
{
    ActionsDAG::NodeRawConstPtrs result;

    if (auto * expression_list_node = expression_node->as<ListNode>())
    {
        for (auto & node : expression_list_node->getNodes())
        {
            auto [node_name, _] = visitImpl(node);
            result.push_back(actions_stack.front().getNodeOrThrow(node_name));
        }
    }
    else
    {
        auto [node_name, _] = visitImpl(expression_node);
        result.push_back(actions_stack.front().getNodeOrThrow(node_name));
    }

    return result;
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitImpl(QueryTreeNodePtr node)
{
    if (auto * column_node = node->as<ColumnNode>())
        return visitColumn(node);
    else if (auto * constant_node = node->as<ConstantNode>())
        return visitConstant(node);
    else if (auto * function_node = node->as<FunctionNode>())
        return visitFunction(node);

    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Expected only column, constant or function node. Actual {}",
        node->formatASTForErrorMessage());
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitColumn(const QueryTreeNodePtr & node)
{
    auto column_node_name = calculateActionsDAGNodeName(node.get(), *planner_context, node_to_node_name);
    const auto & column_node = node->as<ColumnNode &>();

    Int64 actions_stack_size = static_cast<Int64>(actions_stack.size() - 1);
    for (Int64 i = actions_stack_size; i >= 0; --i)
    {
        actions_stack[i].addInputColumnIfNecessary(column_node_name, column_node.getColumnType());

        if (column_node.getColumnSource()->getNodeType() == QueryTreeNodeType::LAMBDA
            && actions_stack[i].getScopeNode().get() == column_node.getColumnSource().get())
        {
            return {column_node_name, i};
        }
    }

    return {column_node_name, 0};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitConstant(const QueryTreeNodePtr & node)
{
    auto constant_node_name = calculateActionsDAGNodeName(node.get(), *planner_context, node_to_node_name);
    const auto & constant_node = node->as<ConstantNode &>();
    const auto & literal = constant_node.getConstantValue();

    ColumnWithTypeAndName column;
    column.name = constant_node_name;
    column.type = constant_node.getResultType();
    column.column = column.type->createColumnConst(1, literal);

    actions_stack[0].addConstantIfNecessary(constant_node_name, column);

    size_t actions_stack_size = actions_stack.size();
    for (size_t i = 1; i < actions_stack_size; ++i)
    {
        auto & actions_stack_node = actions_stack[i];
        actions_stack_node.addInputConstantColumnIfNecessary(constant_node_name, column);
    }

    return {constant_node_name, 0};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitLambda(const QueryTreeNodePtr & node)
{
    auto & lambda_node = node->as<LambdaNode &>();
    auto result_type = lambda_node.getResultType();
    if (!result_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Lambda {} is not resolved during query analysis",
            lambda_node.formatASTForErrorMessage());

    NamesAndTypesList lambda_arguments_names_and_types;

    for (auto & lambda_node_argument : lambda_node.getArguments().getNodes())
    {
        auto lambda_argument_name = lambda_node_argument->getName();
        auto lambda_argument_type = lambda_node_argument->getResultType();
        lambda_arguments_names_and_types.emplace_back(lambda_argument_name, lambda_argument_type);
    }

    size_t previous_scope_node_actions_stack_index = actions_stack.size() - 1;

    auto lambda_actions_dag = std::make_shared<ActionsDAG>();
    actions_stack.emplace_back(lambda_actions_dag, node);

    auto [lambda_expression_node_name, level] = visitImpl(lambda_node.getExpression());
    lambda_actions_dag->getOutputs().push_back(actions_stack.back().getNodeOrThrow(lambda_expression_node_name));
    lambda_actions_dag->removeUnusedActions(Names(1, lambda_expression_node_name));

    auto expression_actions_settings = ExpressionActionsSettings::fromContext(planner_context->getQueryContext(), CompileExpressions::yes);
    auto lambda_actions = std::make_shared<ExpressionActions>(lambda_actions_dag, expression_actions_settings);

    Names captured_column_names;
    ActionsDAG::NodeRawConstPtrs lambda_children;
    Names required_column_names = lambda_actions->getRequiredColumns();

    const auto & lambda_argument_names = lambda_node.getArgumentNames();

    for (const auto & required_column_name : required_column_names)
    {
        auto it = std::find_if(
            lambda_argument_names.begin(), lambda_argument_names.end(), [&](auto & value) { return value == required_column_name; });

        if (it == lambda_argument_names.end())
        {
            lambda_children.push_back(actions_stack[previous_scope_node_actions_stack_index].getNodeOrThrow(required_column_name));
            captured_column_names.push_back(required_column_name);
        }
    }

    auto lambda_node_name = calculateActionsDAGNodeName(node.get(), *planner_context);
    auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
        lambda_actions, captured_column_names, lambda_arguments_names_and_types, result_type, lambda_expression_node_name);
    actions_stack.pop_back();

    if (level == actions_stack.size())
        --level;

    actions_stack[level].addFunctionIfNecessary(lambda_node_name, lambda_children, function_capture);

    size_t actions_stack_size = actions_stack.size();
    for (size_t i = level + 1; i < actions_stack_size; ++i)
    {
        auto & actions_stack_node = actions_stack[i];
        actions_stack_node.addInputColumnIfNecessary(lambda_node_name, result_type);
    }

    return {lambda_node_name, level};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::makeSetForInFunction(const QueryTreeNodePtr & node)
{
    const auto & function_node = node->as<FunctionNode &>();
    auto in_first_argument = function_node.getArguments().getNodes().at(0);
    auto in_second_argument = function_node.getArguments().getNodes().at(1);
    auto in_second_argument_node_type = in_second_argument->getNodeType();

    auto set_source_hash = in_second_argument->getTreeHash();
    String set_key = "__set_" + toString(set_source_hash.first) + '_' + toString(set_source_hash.second);
    auto prepared_set = planner_context->getGlobalPlannerContext()->getSet(set_key);

    if (!prepared_set)
    {
        if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION)
        {
            const auto & settings = planner_context->getQueryContext()->getSettingsRef();
            SizeLimits size_limits_for_set = {settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
            bool tranform_null_in = settings.transform_null_in;

            auto set = std::make_shared<Set>(size_limits_for_set, false /*fill_set_elements*/, tranform_null_in);

            planner_context->getGlobalPlannerContext()->registerSet(set_key, set);
            planner_context->getGlobalPlannerContext()->registerSubqueryNodeForSet(set_key, SubqueryNodeForSet{in_second_argument, set});

            prepared_set = std::move(set);
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::CONSTANT)
        {
            auto & in_second_argument_constant_node = in_second_argument->as<ConstantNode &>();

            const auto & settings = planner_context->getQueryContext()->getSettingsRef();

            auto set = makeSetForConstantValue(
                in_first_argument->getResultType(),
                in_second_argument_constant_node.getResultType(),
                in_second_argument_constant_node.getConstantValue(),
                settings);

            planner_context->getGlobalPlannerContext()->registerSet(set_key, set);
            prepared_set = std::move(set);
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function IN is supported only if second argument is constant or table expression");
        }
    }

    auto column_set = ColumnSet::create(1, std::move(prepared_set));
    auto column_set_const = ColumnConst::create(std::move(column_set), 1);

    ColumnWithTypeAndName column;
    column.name = set_key;
    column.type = std::make_shared<DataTypeSet>();
    column.column = std::move(column_set_const);

    actions_stack[0].addConstantIfNecessary(set_key, column);

    size_t actions_stack_size = actions_stack.size();
    for (size_t i = 1; i < actions_stack_size; ++i)
    {
        auto & actions_stack_node = actions_stack[i];
        actions_stack_node.addInputConstantColumnIfNecessary(set_key, column);
    }

    node_to_node_name.emplace(in_second_argument.get(), set_key);

    return {set_key, 0};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitFunction(const QueryTreeNodePtr & node)
{
    const auto & function_node = node->as<FunctionNode &>();
    std::optional<NodeNameAndNodeMinLevel> in_function_second_argument_node_name_with_level;

    if (function_node.getFunctionName() == "grouping")
    {
        size_t arguments_size = function_node.getArguments().getNodes().size();

        if (arguments_size == 0)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Function GROUPING expects at least one argument");
        else if (arguments_size > 64)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Function GROUPING can have up to 64 arguments, but {} provided",
                arguments_size);

        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Function GROUPING is not supported");
    }
    else if (isNameOfInFunction(function_node.getFunctionName()))
    {
        in_function_second_argument_node_name_with_level = makeSetForInFunction(node);
    }

    const auto & function_arguments = function_node.getArguments().getNodes();
    size_t function_arguments_size = function_arguments.size();

    Names function_arguments_node_names;
    function_arguments_node_names.reserve(function_arguments_size);

    size_t level = 0;
    for (size_t function_argument_index = 0; function_argument_index < function_arguments_size; ++function_argument_index)
    {
        if (in_function_second_argument_node_name_with_level && function_argument_index == 1)
        {
            auto & [node_name, node_min_level] = *in_function_second_argument_node_name_with_level;
            function_arguments_node_names.push_back(std::move(node_name));
            level = std::max(level, node_min_level);
            continue;
        }

        const auto & argument = function_arguments[function_argument_index];

        if (argument->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            auto [node_name, node_min_level] = visitLambda(argument);
            function_arguments_node_names.push_back(std::move(node_name));
            level = std::max(level, node_min_level);
            continue;
        }

        auto [node_name, node_min_level] = visitImpl(argument);
        function_arguments_node_names.push_back(std::move(node_name));
        level = std::max(level, node_min_level);
    }

    auto function_node_name = calculateActionsDAGNodeName(node.get(), *planner_context, node_to_node_name);

    if (function_node.isAggregateFunction())
    {
        size_t actions_stack_size = actions_stack.size();

        for (size_t i = 0; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
        }

        return {function_node_name, 0};
    }

    ActionsDAG::NodeRawConstPtrs children;
    children.reserve(function_arguments_size);

    for (auto & function_argument_node_name : function_arguments_node_names)
        children.push_back(actions_stack[level].getNodeOrThrow(function_argument_node_name));

    if (function_node.getFunctionName() == "arrayJoin")
    {
        if (level != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expression in arrayJoin cannot depend on lambda argument: {} ",
                function_arguments_node_names.at(0));

        actions_stack[level].addArrayJoinIfNecessary(function_node_name, children.at(0));
    }
    else
    {
        actions_stack[level].addFunctionIfNecessary(function_node_name, children, function_node.getFunction());
    }

    size_t actions_stack_size = actions_stack.size();
    for (size_t i = level + 1; i < actions_stack_size; ++i)
    {
        auto & actions_stack_node = actions_stack[i];
        actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
    }

    return {function_node_name, level};
}

}

PlannerActionsVisitor::PlannerActionsVisitor(const PlannerContextPtr & planner_context_)
    : planner_context(planner_context_)
{}

ActionsDAG::NodeRawConstPtrs PlannerActionsVisitor::visit(ActionsDAGPtr actions_dag, QueryTreeNodePtr expression_node)
{
    PlannerActionsVisitorImpl actions_visitor_impl(actions_dag, planner_context);
    return actions_visitor_impl.visit(expression_node);
}

String calculateActionsDAGNodeName(const IQueryTreeNode * node, const PlannerContext & planner_context, QueryTreeNodeToName & node_to_name)
{
    auto it = node_to_name.find(node);
    if (it != node_to_name.end())
        return it->second;

    String result;
    auto node_type = node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::COLUMN:
        {
            const auto * column_identifier = planner_context.getColumnNodeIdentifierOrNull(node);
            result = column_identifier ? *column_identifier : node->getName();

            break;
        }
        case QueryTreeNodeType::CONSTANT:
        {
            std::string constant_value_dump = node->as<ConstantNode &>().getConstantValue().dump();
            result = "__constant_" + constant_value_dump;
            break;
        }
        case QueryTreeNodeType::FUNCTION:
        {
            const auto & function_node = node->as<FunctionNode &>();

            WriteBufferFromOwnString buffer;
            buffer << "__function_" + function_node.getFunctionName();

            const auto & function_parameters_nodes = function_node.getParameters().getNodes();

            if (!function_parameters_nodes.empty())
            {
                buffer << '(';

                size_t function_parameters_nodes_size = function_parameters_nodes.size();
                for (size_t i = 0; i < function_parameters_nodes_size; ++i)
                {
                    const auto & function_parameter_node = function_parameters_nodes[i];
                    calculateActionsDAGNodeName(function_parameter_node.get(), planner_context);

                    if (i + 1 != function_parameters_nodes_size)
                        buffer << ", ";
                }

                buffer << ')';
            }

            const auto & function_arguments_nodes = function_node.getArguments().getNodes();

            buffer << '(';

            size_t function_arguments_nodes_size = function_arguments_nodes.size();
            for (size_t i = 0; i < function_arguments_nodes_size; ++i)
            {
                const auto & function_argument_node = function_arguments_nodes[i];
                buffer << calculateActionsDAGNodeName(function_argument_node.get(), planner_context);

                if (i + 1 != function_arguments_nodes_size)
                    buffer << ", ";
            }

            buffer << ')';

            result = buffer.str();
            break;
        }
        case QueryTreeNodeType::QUERY:
        {
            auto query_hash = node->getTreeHash();

            result = "__subquery_" + std::to_string(query_hash.first) + '_' + std::to_string(query_hash.second);
            break;
        }
        case QueryTreeNodeType::LAMBDA:
        {
            auto lambda_hash = node->getTreeHash();

            result = "__lambda_" + toString(lambda_hash.first) + '_' + toString(lambda_hash.second);
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Invalid action query tree node {}",
                node->formatASTForErrorMessage());
        }
    }

    node_to_name.emplace(node, result);

    return result;
}

String calculateActionsDAGNodeName(const IQueryTreeNode * node, const PlannerContext & planner_context)
{
    QueryTreeNodeToName empty_map;
    return calculateActionsDAGNodeName(node, planner_context, empty_map);
}

}
