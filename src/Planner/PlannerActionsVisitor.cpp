#include <Planner/PlannerActionsVisitor.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantValue.h>

#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeSet.h>

#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionFactory.h>
#include <Functions/indexHint.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

#include <Planner/PlannerContext.h>
#include <Planner/TableExpressionData.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class ActionNodeNameHelper
{
public:
    ActionNodeNameHelper(QueryTreeNodeToName & node_to_name_,
        const PlannerContext & planner_context_,
        bool use_column_identifier_as_action_node_name_)
        : node_to_name(node_to_name_)
        , planner_context(planner_context_)
        , use_column_identifier_as_action_node_name(use_column_identifier_as_action_node_name_)
    {
    }

    String calculateActionNodeName(const QueryTreeNodePtr & node)
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
                const ColumnIdentifier * column_identifier = nullptr;
                if (use_column_identifier_as_action_node_name)
                    column_identifier = planner_context.getColumnNodeIdentifierOrNull(node);

                if (column_identifier)
                {
                    result = *column_identifier;
                }
                else
                {
                    const auto & column_node = node->as<ColumnNode &>();
                    result = column_node.getColumnName();
                }

                break;
            }
            case QueryTreeNodeType::CONSTANT:
            {
                const auto & constant_node = node->as<ConstantNode &>();
                result = calculateConstantActionNodeName(constant_node.getValue(), constant_node.getResultType());
                break;
            }
            case QueryTreeNodeType::FUNCTION:
            {
                const auto & function_node = node->as<FunctionNode &>();
                String in_function_second_argument_node_name;

                if (isNameOfInFunction(function_node.getFunctionName()))
                {
                    const auto & in_first_argument_node = function_node.getArguments().getNodes().at(0);
                    const auto & in_second_argument_node = function_node.getArguments().getNodes().at(1);
                    in_function_second_argument_node_name = planner_context.createSetKey(in_first_argument_node->getResultType(), in_second_argument_node);
                }

                WriteBufferFromOwnString buffer;
                buffer << function_node.getFunctionName();

                const auto & function_parameters_nodes = function_node.getParameters().getNodes();

                if (!function_parameters_nodes.empty())
                {
                    buffer << '(';

                    size_t function_parameters_nodes_size = function_parameters_nodes.size();
                    for (size_t i = 0; i < function_parameters_nodes_size; ++i)
                    {
                        const auto & function_parameter_node = function_parameters_nodes[i];
                        buffer << calculateActionNodeName(function_parameter_node);

                        if (i + 1 != function_parameters_nodes_size)
                            buffer << ", ";
                    }

                    buffer << ')';
                }

                const auto & function_arguments_nodes = function_node.getArguments().getNodes();
                String function_argument_name;

                buffer << '(';

                size_t function_arguments_nodes_size = function_arguments_nodes.size();
                for (size_t i = 0; i < function_arguments_nodes_size; ++i)
                {
                    if (i == 1 && !in_function_second_argument_node_name.empty())
                    {
                        function_argument_name = in_function_second_argument_node_name;
                    }
                    else
                    {
                        const auto & function_argument_node = function_arguments_nodes[i];
                        function_argument_name = calculateActionNodeName(function_argument_node);
                    }

                    buffer << function_argument_name;

                    if (i + 1 != function_arguments_nodes_size)
                        buffer << ", ";
                }

                buffer << ')';

                if (function_node.isWindowFunction())
                {
                    buffer << " OVER (";
                    buffer << calculateWindowNodeActionName(function_node.getWindowNode());
                    buffer << ')';
                }

                result = buffer.str();
                break;
            }
            case QueryTreeNodeType::LAMBDA:
            {
                auto lambda_hash = node->getTreeHash();
                result = "__lambda_" + toString(lambda_hash);
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid action query tree node {}", node->formatASTForErrorMessage());
            }
        }

        node_to_name.emplace(node, result);

        return result;
    }

    static String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type)
    {
        auto constant_name = applyVisitor(FieldVisitorToString(), constant_literal);
        return constant_name + "_" + constant_type->getName();
    }

    static String calculateConstantActionNodeName(const Field & constant_literal)
    {
        return calculateConstantActionNodeName(constant_literal, applyVisitor(FieldToDataType(), constant_literal));
    }

    String calculateWindowNodeActionName(const QueryTreeNodePtr & node)
    {
        auto & window_node = node->as<WindowNode &>();
        WriteBufferFromOwnString buffer;

        if (window_node.hasPartitionBy())
        {
            buffer << "PARTITION BY ";

            auto & partition_by_nodes = window_node.getPartitionBy().getNodes();
            size_t partition_by_nodes_size = partition_by_nodes.size();

            for (size_t i = 0; i < partition_by_nodes_size; ++i)
            {
                auto & partition_by_node = partition_by_nodes[i];
                buffer << calculateActionNodeName(partition_by_node);
                if (i + 1 != partition_by_nodes_size)
                    buffer << ", ";
            }
        }

        if (window_node.hasOrderBy())
        {
            if (window_node.hasPartitionBy())
                buffer << ' ';

            buffer << "ORDER BY ";

            auto & order_by_nodes = window_node.getOrderBy().getNodes();
            size_t order_by_nodes_size = order_by_nodes.size();

            for (size_t i = 0; i < order_by_nodes_size; ++i)
            {
                auto & sort_node = order_by_nodes[i]->as<SortNode &>();
                buffer << calculateActionNodeName(sort_node.getExpression());

                auto sort_direction = sort_node.getSortDirection();
                buffer << (sort_direction == SortDirection::ASCENDING ? " ASC" : " DESC");

                auto nulls_sort_direction = sort_node.getNullsSortDirection();

                if (nulls_sort_direction)
                    buffer << " NULLS " << (nulls_sort_direction == sort_direction ? "LAST" : "FIRST");

                if (auto collator = sort_node.getCollator())
                    buffer << " COLLATE " << collator->getLocale();

                if (sort_node.withFill())
                {
                    buffer << " WITH FILL";

                    if (sort_node.hasFillFrom())
                        buffer << " FROM " << calculateActionNodeName(sort_node.getFillFrom());

                    if (sort_node.hasFillTo())
                        buffer << " TO " << calculateActionNodeName(sort_node.getFillTo());

                    if (sort_node.hasFillStep())
                        buffer << " STEP " << calculateActionNodeName(sort_node.getFillStep());
                }

                if (i + 1 != order_by_nodes_size)
                    buffer << ", ";
            }
        }

        auto & window_frame = window_node.getWindowFrame();
        if (!window_frame.is_default)
        {
            if (window_node.hasPartitionBy() || window_node.hasOrderBy())
                buffer << ' ';

            buffer << window_frame.type << " BETWEEN ";
            if (window_frame.begin_type == WindowFrame::BoundaryType::Current)
            {
                buffer << "CURRENT ROW";
            }
            else if (window_frame.begin_type == WindowFrame::BoundaryType::Unbounded)
            {
                buffer << "UNBOUNDED";
                buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
            }
            else
            {
                buffer << calculateActionNodeName(window_node.getFrameBeginOffsetNode());
                buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
            }

            buffer << " AND ";

            if (window_frame.end_type == WindowFrame::BoundaryType::Current)
            {
                buffer << "CURRENT ROW";
            }
            else if (window_frame.end_type == WindowFrame::BoundaryType::Unbounded)
            {
                buffer << "UNBOUNDED";
                buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
            }
            else
            {
                buffer << calculateActionNodeName(window_node.getFrameEndOffsetNode());
                buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
            }
        }

        return buffer.str();
    }
private:
    std::unordered_map<QueryTreeNodePtr, std::string> & node_to_name;
    const PlannerContext & planner_context;
    bool use_column_identifier_as_action_node_name = true;
};

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

    [[maybe_unused]] bool containsInputNode(const std::string & node_name)
    {
        const auto * node = tryGetNode(node_name);
        if (node && node->type == ActionsDAG::ActionType::INPUT)
            return true;

        return false;
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

    template <typename FunctionOrOverloadResolver>
    const ActionsDAG::Node * addFunctionIfNecessary(const std::string & node_name, ActionsDAG::NodeRawConstPtrs children, const FunctionOrOverloadResolver & function)
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
    PlannerActionsVisitorImpl(ActionsDAGPtr actions_dag,
        const PlannerContextPtr & planner_context_,
        bool use_column_identifier_as_action_node_name_);

    ActionsDAG::NodeRawConstPtrs visit(QueryTreeNodePtr expression_node);

private:
    using NodeNameAndNodeMinLevel = std::pair<std::string, size_t>;

    NodeNameAndNodeMinLevel visitImpl(QueryTreeNodePtr node);

    NodeNameAndNodeMinLevel visitColumn(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitConstant(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitLambda(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel makeSetForInFunction(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitIndexHintFunction(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitFunction(const QueryTreeNodePtr & node);

    std::vector<ActionsScopeNode> actions_stack;
    std::unordered_map<QueryTreeNodePtr, std::string> node_to_node_name;
    const PlannerContextPtr planner_context;
    ActionNodeNameHelper action_node_name_helper;
};

PlannerActionsVisitorImpl::PlannerActionsVisitorImpl(ActionsDAGPtr actions_dag,
    const PlannerContextPtr & planner_context_,
    bool use_column_identifier_as_action_node_name_)
    : planner_context(planner_context_)
    , action_node_name_helper(node_to_node_name, *planner_context, use_column_identifier_as_action_node_name_)
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
    auto node_type = node->getNodeType();

    if (node_type == QueryTreeNodeType::COLUMN)
        return visitColumn(node);
    else if (node_type == QueryTreeNodeType::CONSTANT)
        return visitConstant(node);
    else if (node_type == QueryTreeNodeType::FUNCTION)
        return visitFunction(node);

    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Expected column, constant, function. Actual {}",
        node->formatASTForErrorMessage());
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitColumn(const QueryTreeNodePtr & node)
{
    auto column_node_name = action_node_name_helper.calculateActionNodeName(node);
    const auto & column_node = node->as<ColumnNode &>();

    Int64 actions_stack_size = static_cast<Int64>(actions_stack.size() - 1);
    for (Int64 i = actions_stack_size; i >= 0; --i)
    {
        actions_stack[i].addInputColumnIfNecessary(column_node_name, column_node.getColumnType());

        auto column_source = column_node.getColumnSourceOrNull();
        if (column_source &&
            column_source->getNodeType() == QueryTreeNodeType::LAMBDA &&
            actions_stack[i].getScopeNode().get() == column_source.get())
        {
            return {column_node_name, i};
        }
    }

    return {column_node_name, 0};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitConstant(const QueryTreeNodePtr & node)
{
    const auto & constant_node = node->as<ConstantNode &>();
    const auto & constant_literal = constant_node.getValue();
    const auto & constant_type = constant_node.getResultType();

    auto constant_node_name = calculateConstantActionNodeName(constant_literal, constant_type);

    ColumnWithTypeAndName column;
    column.name = constant_node_name;
    column.type = constant_type;
    column.column = column.type->createColumnConst(1, constant_literal);

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

    auto & lambda_arguments_nodes = lambda_node.getArguments().getNodes();
    size_t lambda_arguments_nodes_size = lambda_arguments_nodes.size();

    NamesAndTypesList lambda_arguments_names_and_types;

    for (size_t i = 0; i < lambda_arguments_nodes_size; ++i)
    {
        const auto & lambda_argument_name = lambda_node.getArgumentNames().at(i);
        auto lambda_argument_type = lambda_arguments_nodes[i]->getResultType();
        lambda_arguments_names_and_types.emplace_back(lambda_argument_name, std::move(lambda_argument_type));
    }

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

    if (level == actions_stack.size() - 1)
        --level;

    const auto & lambda_argument_names = lambda_node.getArgumentNames();

    for (const auto & required_column_name : required_column_names)
    {
        auto it = std::find(lambda_argument_names.begin(), lambda_argument_names.end(), required_column_name);

        if (it == lambda_argument_names.end())
        {
            lambda_children.push_back(actions_stack[level].getNodeOrThrow(required_column_name));
            captured_column_names.push_back(required_column_name);
        }
    }

    auto lambda_node_name = calculateActionNodeName(node, *planner_context);
    auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
        lambda_actions, captured_column_names, lambda_arguments_names_and_types, lambda_node.getExpression()->getResultType(), lambda_expression_node_name);
    actions_stack.pop_back();

    // TODO: Pass IFunctionBase here not FunctionCaptureOverloadResolver.
    const auto * actions_node = actions_stack[level].addFunctionIfNecessary(lambda_node_name, std::move(lambda_children), function_capture);

    if (!result_type->equals(*actions_node->result_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Lambda resolved type {} is not equal to type from actions DAG {}",
            result_type, actions_node->result_type);

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

    DataTypes set_element_types;

    auto in_second_argument_node_type = in_second_argument->getNodeType();

    bool subquery_or_table =
        in_second_argument_node_type == QueryTreeNodeType::QUERY ||
        in_second_argument_node_type == QueryTreeNodeType::UNION ||
        in_second_argument_node_type == QueryTreeNodeType::TABLE;

    FutureSetPtr set;
    auto set_key = in_second_argument->getTreeHash();

    if (!subquery_or_table)
    {
        set_element_types = {in_first_argument->getResultType()};
        const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(set_element_types.front().get());
        if (left_tuple_type && left_tuple_type->getElements().size() != 1)
            set_element_types = left_tuple_type->getElements();

        set_element_types = Set::getElementTypes(std::move(set_element_types), planner_context->getQueryContext()->getSettingsRef().transform_null_in);
        set = planner_context->getPreparedSets().findTuple(set_key, set_element_types);
    }
    else
    {
        set = planner_context->getPreparedSets().findSubquery(set_key);
        if (!set)
            set = planner_context->getPreparedSets().findStorage(set_key);
    }

    if (!set)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "No set is registered for key {}",
            PreparedSets::toString(set_key, set_element_types));

    ColumnWithTypeAndName column;
    column.name = planner_context->createSetKey(in_first_argument->getResultType(), in_second_argument);
    column.type = std::make_shared<DataTypeSet>();

    bool set_is_created = set->get() != nullptr;
    auto column_set = ColumnSet::create(1, std::move(set));

    if (set_is_created)
        column.column = ColumnConst::create(std::move(column_set), 1);
    else
        column.column = std::move(column_set);

    actions_stack[0].addConstantIfNecessary(column.name, column);

    size_t actions_stack_size = actions_stack.size();
    for (size_t i = 1; i < actions_stack_size; ++i)
    {
        auto & actions_stack_node = actions_stack[i];
        actions_stack_node.addInputConstantColumnIfNecessary(column.name, column);
    }

    return {column.name, 0};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitIndexHintFunction(const QueryTreeNodePtr & node)
{
    const auto & function_node = node->as<FunctionNode &>();
    auto function_node_name = action_node_name_helper.calculateActionNodeName(node);

    auto index_hint_actions_dag = std::make_shared<ActionsDAG>();
    auto & index_hint_actions_dag_outputs = index_hint_actions_dag->getOutputs();
    std::unordered_set<std::string_view> index_hint_actions_dag_output_node_names;
    PlannerActionsVisitor actions_visitor(planner_context);

    for (const auto & argument : function_node.getArguments())
    {
        auto index_hint_argument_expression_dag_nodes = actions_visitor.visit(index_hint_actions_dag, argument);

        for (auto & expression_dag_node : index_hint_argument_expression_dag_nodes)
        {
            if (index_hint_actions_dag_output_node_names.contains(expression_dag_node->result_name))
                continue;

            index_hint_actions_dag_output_node_names.insert(expression_dag_node->result_name);
            index_hint_actions_dag_outputs.push_back(expression_dag_node);
        }
    }

    auto index_hint_function = std::make_shared<FunctionIndexHint>();
    index_hint_function->setActions(std::move(index_hint_actions_dag));
    auto index_hint_function_overload_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(index_hint_function));

    size_t index_hint_function_level = actions_stack.size() - 1;
    actions_stack[index_hint_function_level].addFunctionIfNecessary(function_node_name, {}, index_hint_function_overload_resolver);

    return {function_node_name, index_hint_function_level};
}

PlannerActionsVisitorImpl::NodeNameAndNodeMinLevel PlannerActionsVisitorImpl::visitFunction(const QueryTreeNodePtr & node)
{
    const auto & function_node = node->as<FunctionNode &>();
    if (function_node.getFunctionName() == "indexHint")
        return visitIndexHintFunction(node);

    std::optional<NodeNameAndNodeMinLevel> in_function_second_argument_node_name_with_level;

    if (isNameOfInFunction(function_node.getFunctionName()))
        in_function_second_argument_node_name_with_level = makeSetForInFunction(node);

    auto function_node_name = action_node_name_helper.calculateActionNodeName(node);

    /* Aggregate functions, window functions, and GROUP BY expressions were already analyzed in the previous steps.
     * If we have already visited some expression, we don't need to revisit it or its arguments again.
     * For example, the expression from the aggregation step is also present in the projection:
     *    SELECT foo(a, b, c) as x FROM table GROUP BY foo(a, b, c)
     * In this case we should not analyze `a`, `b`, `c` again.
     * Moreover, it can lead to an error if we have arrayJoin in the arguments because it will be calculated twice.
     */
    bool is_input_node = function_node.isAggregateFunction() || function_node.isWindowFunction()
        || actions_stack.front().containsInputNode(function_node_name);
    if (is_input_node)
    {
        size_t actions_stack_size = actions_stack.size();

        for (size_t i = 0; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
        }

        return {function_node_name, 0};
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
        actions_stack[level].addFunctionIfNecessary(function_node_name, children, function_node);
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

PlannerActionsVisitor::PlannerActionsVisitor(const PlannerContextPtr & planner_context_, bool use_column_identifier_as_action_node_name_)
    : planner_context(planner_context_)
    , use_column_identifier_as_action_node_name(use_column_identifier_as_action_node_name_)
{}

ActionsDAG::NodeRawConstPtrs PlannerActionsVisitor::visit(ActionsDAGPtr actions_dag, QueryTreeNodePtr expression_node)
{
    PlannerActionsVisitorImpl actions_visitor_impl(actions_dag, planner_context, use_column_identifier_as_action_node_name);
    return actions_visitor_impl.visit(expression_node);
}

String calculateActionNodeName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name,
    bool use_column_identifier_as_action_node_name)
{
    ActionNodeNameHelper helper(node_to_name, planner_context, use_column_identifier_as_action_node_name);
    return helper.calculateActionNodeName(node);
}

String calculateActionNodeName(const QueryTreeNodePtr & node, const PlannerContext & planner_context, bool use_column_identifier_as_action_node_name)
{
    QueryTreeNodeToName empty_map;
    ActionNodeNameHelper helper(empty_map, planner_context, use_column_identifier_as_action_node_name);
    return helper.calculateActionNodeName(node);
}

String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type)
{
    return ActionNodeNameHelper::calculateConstantActionNodeName(constant_literal, constant_type);
}

String calculateConstantActionNodeName(const Field & constant_literal)
{
    return ActionNodeNameHelper::calculateConstantActionNodeName(constant_literal);
}

String calculateWindowNodeActionName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name,
    bool use_column_identifier_as_action_node_name)
{
    ActionNodeNameHelper helper(node_to_name, planner_context, use_column_identifier_as_action_node_name);
    return helper.calculateWindowNodeActionName(node);
}

String calculateWindowNodeActionName(const QueryTreeNodePtr & node, const PlannerContext & planner_context, bool use_column_identifier_as_action_node_name)
{
    QueryTreeNodeToName empty_map;
    ActionNodeNameHelper helper(empty_map, planner_context, use_column_identifier_as_action_node_name);
    return helper.calculateWindowNodeActionName(node);
}

}
