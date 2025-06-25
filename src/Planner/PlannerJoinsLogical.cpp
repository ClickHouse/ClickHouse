#include <Planner/PlannerJoinsLogical.h>
#include <Planner/PlannerJoins.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageDictionary.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/isNotDistinctFrom.h>

#include <Analyzer/Utils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>

#include <Dictionaries/IDictionary.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/DirectJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/PasteJoin.h>
#include <Interpreters/JoinExpressionActions.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/JoinOperator.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>

#include <memory>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

namespace Setting
{
    extern const SettingsBool join_use_nulls;
    extern const SettingsBool allow_general_join_planning;
    extern const SettingsJoinAlgorithm join_algorithm;
}

const ActionsDAG::Node * appendExpression(
    ActionsDAG & dag,
    const QueryTreeNodePtr & expression,
    const PlannerContextPtr & planner_context)
{
    size_t input_count = dag.getInputs().size();

    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    PlannerActionsVisitor join_expression_visitor(planner_context, empty_correlated_columns_set);
    auto [join_expression_dag_node_raw_pointers, correlated_subtrees] = join_expression_visitor.visit(dag, expression);
    correlated_subtrees.assertEmpty("in join expression");

    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expression {} expected be a single node, got {}",
            expression->formatASTForErrorMessage(), dag.dumpDAG());

    if (input_count != dag.getInputs().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unknown inputs added to actions dag:\n{}\nAfter adding expression {} with query tree node:\n{}",
            dag.dumpDAG(), expression->formatASTForErrorMessage(), expression->dumpTree());

    return join_expression_dag_node_raw_pointers[0];
}

struct JoinOperatorBuildContext
{
    explicit JoinOperatorBuildContext(
        const JoinNode & join_node_,
        const Block & left_header,
        const Block & right_header,
        const PlannerContextPtr & planner_context_)
        : join_node(join_node_)
        , planner_context(planner_context_)
        , left_table_expression_set(extractTableExpressionsSet(join_node.getLeftTableExpression()))
        , right_table_expression_set(extractTableExpressionsSet(join_node.getRightTableExpression()))
        , expression_actions(left_header, right_header)
        , join_operator(join_node.getKind(), join_node.getStrictness(), join_node.getLocality())
    {
    }

    enum class JoinSource : uint8_t { None, Left, Right, Both };

    JoinSource getExpressionSource(const QueryTreeNodePtr & node)
    {
        auto res = extractJoinTableSidesFromExpression(node.get(), left_table_expression_set, right_table_expression_set, join_node);
        if (res.empty())
            return JoinSource::None;
        if (res.size() == 1)
        {
            if (*res.begin() == JoinTableSide::Left)
                return JoinSource::Left;
            return JoinSource::Right;
        }
        return JoinSource::Both;
    }

    JoinActionRef addExpression(const QueryTreeNodePtr & node)
    {
        auto dag = expression_actions.getActionsDAG();
        const auto * dag_node = appendExpression(*dag, node, planner_context);
        return JoinActionRef(dag_node, expression_actions);
    }

    const JoinNode & join_node;
    const PlannerContextPtr & planner_context;

    TableExpressionSet left_table_expression_set;
    TableExpressionSet right_table_expression_set;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;
};

struct JoinCondition
{
    ActionsDAG::NodeRawConstPtrs conjuncts;

    const ActionsDAG::Node * concat(ActionsDAG & dag)
    {
        if (conjuncts.empty())
            return nullptr;

        if (conjuncts.size() == 1)
            return conjuncts.front();

        auto func_builder_and = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

        return &dag.addFunction(func_builder_and, conjuncts, {});
    }
};

std::unordered_map<String, const ActionsDAG::Node *>
buildJoinUsingCondition(const QueryTreeNodePtr & node, JoinOperatorBuildContext & builder_context)
{
    JoinActionRef::AddFunction operator_function(JoinConditionOperator::Equals);

    auto & using_nodes = node->as<ListNode &>().getNodes();
    size_t num_nodes = using_nodes.size();

    auto & join_operator = builder_context.join_operator;

    std::unordered_map<String, const ActionsDAG::Node *> changed_types;

    JoinActionRef::AddFunction using_concat_function(FunctionFactory::instance().get("firstTruthy", nullptr));
    for (size_t i = 0; i < num_nodes; ++i)
    {
        auto & using_column_node = using_nodes[i]->as<ColumnNode &>();
        auto & inner_columns_list = using_column_node.getExpressionOrThrow()->as<ListNode &>();
        // chassert(inner_columns_list.getNodes().size() == 2);

        std::vector<JoinActionRef> args;
        const auto & inner_columns = inner_columns_list.getNodes();
        if (inner_columns.size() < 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Using column {} expected to have at least 2 inner columns, in query tree {}",
                using_column_node.getColumnName(), node->dumpTree());

        // for (const auto & inner_node : inner_columns)
        // {
        //     const auto & inner_column = inner_node->as<ColumnNode &>();

        //     const auto & table_expression_data = builder_context.planner_context->getTableExpressionDataOrThrow(inner_column.getColumnSource());
        //     const auto & column_identifier = table_expression_data.getColumnIdentifierOrThrow(inner_column.getColumnName());
        //     changed_types[column_identifier] = using_column_node.getResultType();
        // }

        const auto & result_type = using_column_node.getResultType();
        auto cast_to_super = [&result_type](auto & dag, auto && nodes) { return &dag.addCast(*nodes.at(0), result_type, {}); };

        for (const auto & inner_column : inner_columns)
        {
            auto & arg = args.emplace_back(builder_context.addExpression(inner_column));
            if (!arg.getType()->equals(*result_type))
            {
                String input_column_name = arg.getColumnName();
                arg = JoinActionRef::transform({arg}, cast_to_super);
                changed_types[input_column_name] = arg.getNode();
            }

            if (arg.fromNone())
                arg.setSourceRelations(BitSet().set(0));
        }

        auto rhs = args.back();
        args.pop_back();

        auto lhs = args.size() == 1 ? args.front() : JoinActionRef::transform(args, using_concat_function);

        /// For ASOF join, the last column in USING list is the ASOF column
        if (join_operator.strictness == JoinStrictness::Asof && i == num_nodes - 1)
            operator_function = JoinActionRef::AddFunction(JoinConditionOperator::GreaterOrEquals);

        auto op = JoinActionRef::transform({lhs, rhs}, operator_function);
        join_operator.expression.emplace_back(op);
    }

    return changed_types;
}

void buildJoinCondition(const QueryTreeNodePtr & node, JoinOperatorBuildContext & builder_context, JoinCondition & join_condition)
{
    std::string function_name;
    const auto * function_node = node->as<FunctionNode>();
    if (function_node)
        function_name = function_node->getFunction()->getName();

    if (function_name == "and")
    {
        for (const auto & child : function_node->getArguments())
            buildJoinCondition(child, builder_context, join_condition);
        return;
    }

    join_condition.conjuncts.push_back(builder_context.addExpression(node).getNode());
}

void buildDisjunctiveJoinConditions(const QueryTreeNodePtr & node, JoinOperatorBuildContext & builder_context, std::vector<JoinCondition> & join_conditions)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            node->formatASTForErrorMessage());

    const auto & function_name = function_node->getFunction()->getName();

    if (function_name == "or")
    {
        for (const auto & child : function_node->getArguments())
            buildDisjunctiveJoinConditions(child, builder_context, join_conditions);
        return;
    }
    buildJoinCondition(node, builder_context, join_conditions.emplace_back());
}


void addConditionsToJoinOperator(JoinOperatorBuildContext & build_context, std::vector<JoinCondition> join_conditions)
{
    if (join_conditions.size() == 1)
    {
        for (const auto * condition : join_conditions.at(0).conjuncts)
            build_context.join_operator.expression.push_back(JoinActionRef(condition, build_context.expression_actions));
        return;
    }

    auto func_builder_or = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    auto actions_dag = build_context.expression_actions.getActionsDAG();
    auto nodes = std::ranges::to<std::vector>(std::views::transform(join_conditions, [&](JoinCondition & condition) { return condition.concat(*actions_dag); }));
    const auto * result_node = &actions_dag->addFunction(func_builder_or, nodes, {});
    build_context.join_operator.expression.push_back(JoinActionRef(result_node, build_context.expression_actions));
}


void buildDisjunctiveJoinConditions(const QueryTreeNodePtr & node, JoinOperatorBuildContext & build_context)
{
    std::vector<JoinCondition> join_conditions;
    buildDisjunctiveJoinConditions(node, build_context, join_conditions);
    addConditionsToJoinOperator(build_context, std::move(join_conditions));
}

static bool hasEquiConditions(const JoinCondition & condition)
{
    for (const auto & conjunct : condition.conjuncts)
    {
        String function_name = conjunct->function ? conjunct->function->getName() : String();
        if (function_name == NameEquals::name ||
            function_name == FunctionIsNotDistinctFrom::name)
            return true;
    }
    return false;
}


JoinCondition concatConditions(const JoinCondition & lhs, const JoinCondition & rhs)
{
    JoinCondition result = lhs;
    result.conjuncts.insert(result.conjuncts.end(), rhs.conjuncts.begin(), rhs.conjuncts.end());
    return result;
}

static std::vector<JoinCondition> makeCrossProduct(const std::vector<JoinCondition> & lhs, const std::vector<JoinCondition> & rhs)
{
    std::vector<JoinCondition> result;
    for (const auto & rhs_clause : rhs)
    {
        for (const auto & lhs_clause : lhs)
        {
            result.emplace_back(concatConditions(lhs_clause, rhs_clause));
        }
    }
    return result;
}


void buildDisjunctiveJoinConditionsGeneral(const QueryTreeNodePtr & join_expression, JoinOperatorBuildContext & builder_context)
{
    using JoinConditions = std::vector<JoinCondition>;
    if (join_expression->getNodeType() != QueryTreeNodeType::FUNCTION)
    {
        std::vector<JoinCondition> join_conditions;
        buildJoinCondition(join_expression, builder_context, join_conditions.emplace_back());
        addConditionsToJoinOperator(builder_context, std::move(join_conditions));
        return;
    }

    std::unordered_map<const IQueryTreeNode *, JoinConditions> built_clauses;
    std::stack<QueryTreeNodePtr> nodes_to_process;
    nodes_to_process.push(join_expression);

    auto get_and_check_built_clause = [&built_clauses](const IQueryTreeNode * node) -> JoinConditions &
    {
        auto it = built_clauses.find(node);
        if (it == built_clauses.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join clauses are not built for node: {}", node->formatASTForErrorMessage());

        if (it->second.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join clauses are already used for node: {}", node->formatASTForErrorMessage());

        return it->second;
    };
    while (!nodes_to_process.empty())
    {
        auto node = nodes_to_process.top();
        auto * function_node = node->as<FunctionNode>();
        const auto function_name = function_node ? function_node->getFunctionName() : String();
        auto expression_source = builder_context.getExpressionSource(node);

        // If the expression is a logical expression and it contains expressions from both sides, let's combine the clauses, otherwise let's just build one join clause
        bool is_complex = (function_name == "and" || function_name == "or") && expression_source == JoinOperatorBuildContext::JoinSource::Both;
        if (is_complex)
        {
            auto & arguments = function_node->getArguments().getNodes();
            auto * first_argument = arguments.front().get();
            if (const auto it = built_clauses.find(first_argument); it == built_clauses.end())
            {
                for (auto & argument : arguments)
                    nodes_to_process.push(argument);

                continue;
            }

            nodes_to_process.pop();

            std::vector<JoinCondition> result;
            if (function_name == "or")
            {
                for (auto & argument : arguments)
                {
                    auto & child_res = get_and_check_built_clause(argument.get());
                    result.insert(result.end(), std::make_move_iterator(child_res.begin()), std::make_move_iterator(child_res.end()));
                    child_res.clear();
                }

                // When some expressions have key expressions and some doesn't, then let's plan the whole OR expression as a single clause to eliminate the chance that some clauses might end up without key expressions
                // TODO(antaljanosbenjamin/vdimir): Analyze the expressions first, so join clauses are not built unnecessarily.
                size_t with_key_expression = static_cast<size_t>(std::count_if(result.begin(), result.end(), hasEquiConditions));
                if (result.size() > 1 && with_key_expression != 0 && with_key_expression < result.size())
                {
                    result.clear();
                    buildJoinCondition(node, builder_context, result.emplace_back());
                }
            }
            else
            {
                auto it = arguments.begin();
                {
                    auto & child_res = get_and_check_built_clause(it->get());

                    result.insert(result.end(), std::make_move_iterator(child_res.begin()), std::make_move_iterator(child_res.end()));
                    child_res.clear();
                }
                it++;

                for (; it != arguments.end(); it++)
                {
                    auto & child_res = get_and_check_built_clause(it->get());
                    result = makeCrossProduct(result, child_res);
                    child_res.clear();
                }
            }
            built_clauses.emplace(node.get(), std::move(result));
        }
        else
        {
            nodes_to_process.pop();
            std::vector<JoinCondition> clauses;
            buildJoinCondition(node, builder_context, clauses.emplace_back());
            built_clauses.emplace(node.get(), std::move(clauses));
        }
    }

    addConditionsToJoinOperator(builder_context, std::move(built_clauses.at(join_expression.get())));
}

std::unique_ptr<JoinStepLogical> buildJoinStepLogical(
    const Block & left_header,
    const Block & right_header,
    const NameSet & outer_scope_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    JoinOperatorBuildContext build_context(join_node, left_header, right_header, planner_context);

    const auto & join_on_expression = join_node.getJoinExpression();
    auto join_expression_constant = tryExtractConstantFromConditionNode(join_on_expression);
    if (join_on_expression && join_on_expression->getNodeType() == QueryTreeNodeType::CONSTANT && !join_expression_constant.has_value())
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Wrong type {} of JOIN expression {}",
            join_on_expression->getResultType()->getName(), join_on_expression->formatASTForErrorMessage());
    }

    auto join_expression_node = getJoinExpressionFromNode(join_node);

    const auto & query_settings = build_context.planner_context->getQueryContext()->getSettingsRef();
    const auto & join_algorithms = query_settings[Setting::join_algorithm];

    std::unordered_map<String, const ActionsDAG::Node *> changed_types;
    /// CROSS/PASTE JOIN: doesn't have expression
    if (join_expression_node == nullptr)
    {
        if (!isCrossOrComma(join_node.getKind()) && !isPaste(join_node.getKind()))
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Missing join expression in {}", join_node.formatASTForErrorMessage());
    }
    /// USING
    else if (join_node.isUsingJoinExpression())
    {
        changed_types = buildJoinUsingCondition(join_expression_node, build_context);
    }
    /// JOIN ON non-constant expression
    else if (!join_expression_constant.has_value() || build_context.join_operator.strictness == JoinStrictness::Asof)
    {
        if (join_expression_node->getNodeType() != QueryTreeNodeType::FUNCTION)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression expected function",
                join_node.formatASTForErrorMessage());

        bool use_general_join_planning = (TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH)
            || TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::AUTO)) && query_settings[Setting::allow_general_join_planning];

        if (use_general_join_planning)
            buildDisjunctiveJoinConditionsGeneral(join_expression_node, build_context);
        else
            buildDisjunctiveJoinConditions(join_expression_node, build_context);
    }
    else if (join_expression_constant.has_value())
    {
        if (!TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JOIN ON constant supported only with join algorithm 'hash'");

        bool join_expression_value = join_expression_constant.value();
        if (!join_expression_value)
        {
            auto actions_dag = build_context.expression_actions.getActionsDAG();
            auto nothing_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            ColumnWithTypeAndName null_column(nothing_type->createColumnConstWithDefaultValue(1), nothing_type, "NULL");
            JoinActionRef null_action(&actions_dag->addColumn(null_column), build_context.expression_actions);
            null_action.setSourceRelations(BitSet().set(0).set(1));
            build_context.join_operator.expression.push_back(null_action);
        }
    }

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();
    auto join_step = std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        std::move(build_context.join_operator),
        std::move(build_context.expression_actions),
        outer_scope_columns,
        changed_types,
        settings[Setting::join_use_nulls],
        JoinSettings(settings),
        SortingStep::Settings(settings));
    return join_step;
}

PreparedJoinStorage tryGetStorageInTableJoin(const QueryTreeNodePtr & table_expression, const PlannerContextPtr & planner_context)
{
    StoragePtr storage;

    if (auto * table_node = table_expression->as<TableNode>())
        storage = table_node->getStorage();
    else if (auto * table_function = table_expression->as<TableFunctionNode>())
        storage = table_function->getStorage();
    else
        return {};

    PreparedJoinStorage result;
    const auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
    result.column_mapping = table_expression_data.getColumnIdentifierToColumnName();

    result.storage_join = std::dynamic_pointer_cast<StorageJoin>(storage);
    if (result.storage_join)
        return result;

    auto storage_dictionary = std::dynamic_pointer_cast<StorageDictionary>(storage);
    if (storage_dictionary && storage_dictionary->getDictionary()->getSpecialKeyType() != DictionarySpecialKeyType::Range)
    {
        result.storage_key_value = std::dynamic_pointer_cast<const IKeyValueEntity>(storage_dictionary->getDictionary());
        return result;
    }

    result.storage_key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
    if (result.storage_key_value)
        return result;

    return {};
}

}


// static String getQueryDisplayLabel(const QueryTreeNodePtr & node)
// {
//     const auto & printable_alias = node->getOriginalAlias();
//     const auto & internal_alias = node->getAlias();
//     if (!printable_alias.empty() && printable_alias != internal_alias)
//         return printable_alias;

//     if (const auto * table_node = node->as<TableNode>(); table_node && table_node->hasOriginalAST())
//     {
//         auto result = table_node->getOriginalAST()->formatForLogging();
//         boost::trim(result);
//         return result;
//     }

//     return {};
// }
