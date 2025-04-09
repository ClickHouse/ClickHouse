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

#include <Analyzer/Utils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>

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
#include <Processors/QueryPlan/QueryPlan.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Planner/PlannerJoinTree.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/JoinOperator.h>
#include <ranges>

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
    PlannerActionsVisitor join_expression_visitor(planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(dag, expression);
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expression {} expected be a single node, got {}",
            expression->formatASTForErrorMessage(), dag.dumpDAG());

    dag.addOrReplaceInOutputs(*join_expression_dag_node_raw_pointers[0]);
    return join_expression_dag_node_raw_pointers[0];
}

auto dumpTableExpressionMapping(const std::unordered_map<const IQueryTreeNode *, size_t> & table_expression_mapping)
{
    auto format_entry = [](const auto & e) { return fmt::format("{} -> {}", e.second, e.first->formatASTForErrorMessage()); };
    return fmt::join(table_expression_mapping | std::views::transform(format_entry), ", ");
}

struct JoinFlattenContext
{
    explicit JoinFlattenContext(
        const JoinNode & join_node_,
        JoinStepLogical & join_step_,
        JoinOperator & join_operator_,
        std::unordered_map<const IQueryTreeNode *, size_t> & table_expression_to_join_input_mapping_,
        const PlannerContextPtr & planner_context_)
        : join_step(join_step_)
        , join_operator(join_operator_)
        , join_node(join_node_)
        , table_expression_to_join_input_mapping(table_expression_to_join_input_mapping_)
        , planner_context(planner_context_)
    {
        for (auto && header : join_step.getCurrentHeaders())
            tables.emplace_back(header.getColumnsWithTypeAndName());
    }

    // ColumnsWithTypeAndName mapUsingColumns(
    //     const JoinNode & join_node_,
    //     const PlannerContextPtr & planner_context_)
    // {
    //     if (join_node_.isUsingJoinExpression())
    //     {
    //         auto & using_list = join_node_.getJoinExpression()->as<ListNode &>();
    //         for (auto & join_using_node : using_list.getNodes())
    //         {
    //             auto & column_node = join_using_node->as<ColumnNode &>();
    //             const auto & column_node_sources = column_node.getExpressionOrThrow()->as<ListNode &>().getNodes();
    //             auto left_source_name = planner_context_->getColumnNodeIdentifierOrThrow(column_node_sources.getNodes().at(0));
    //             auto right_source_name = planner_context_->getColumnNodeIdentifierOrThrow(column_node_sources.getNodes().at(1));

    //             auto & using_columns_mapping = join_step->getUsingColumnsMapping();
    //             using_columns_mapping.emplace_back({left_source_name, right_source_name});
    //         }
    //     }
    // }

    BaseRelsSet getExpressionSource(const QueryTreeNodePtr & node)
    {
        auto source_table_expressions = getExpressionSourceSet(node.get());
        BaseRelsSet res;
        for (const auto * table_expression : source_table_expressions)
        {
            auto it = table_expression_to_join_input_mapping.find(table_expression);
            if (it == table_expression_to_join_input_mapping.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "JOIN {} ON clause contains column {} that do not exist join tree: {}",
                    join_node.formatASTForErrorMessage(),
                    table_expression->formatASTForErrorMessage(),
                    dumpTableExpressionMapping(table_expression_to_join_input_mapping));

            res.set(it->second);
        }
        return res;
    }

    BaseRelsSet getLeftMask() const
    {
        size_t current_index = tables.size() - 1;
        chassert(current_index >= 1);
        return BaseRelsSet((1u << current_index) - 1);
    }

    BaseRelsSet getRightMask() const
    {
        size_t current_index = tables.size() - 1;
        chassert(current_index >= 1);
        return BaseRelsSet(1u << current_index);
    }

    size_t getCurrentRelIndex() const
    {
        return tables.size() - 1;
    }

    JoinActionRef addExpression(const QueryTreeNodePtr & node, BaseRelsSet sources)
    {
        ActionsDAG & actions_dag = join_step.getExpressionActions();
        return JoinActionRef(appendExpression(actions_dag, node, planner_context), &actions_dag, sources, node->formatASTForErrorMessage());
    }

    JoinStepLogical & join_step;
    JoinOperator & join_operator;

    const JoinNode & join_node;

    std::unordered_map<const IQueryTreeNode *, size_t> & table_expression_to_join_input_mapping;

    std::vector<ColumnsWithTypeAndName> tables;

    const PlannerContextPtr & planner_context;
};


bool tryGetJoinPredicate(const FunctionNode * function_node, JoinFlattenContext & join_flatten, JoinCondition & join_condition)
{
    if (!function_node || function_node->getArguments().getNodes().size() != 2)
        return false;

    auto predicate_operator = getJoinPredicateOperator(function_node->getFunctionName());
    if (!predicate_operator.has_value())
        return false;

    auto left_node = function_node->getArguments().getNodes().at(0);
    auto left_expr_source = join_flatten.getExpressionSource(left_node);

    auto right_node = function_node->getArguments().getNodes().at(1);
    auto right_expr_source = join_flatten.getExpressionSource(right_node);

    auto left_mask = join_flatten.getLeftMask();
    auto right_mask = join_flatten.getRightMask();

    if (isSubsetOf(left_expr_source, left_mask) && isSubsetOf(right_expr_source, right_mask))
    {
        join_condition.predicates.emplace_back(JoinPredicate{
            join_flatten.addExpression(left_node, left_expr_source),
            join_flatten.addExpression(right_node, right_expr_source),
            predicate_operator.value()});
        return true;
    }

    if (isSubsetOf(left_expr_source, right_mask) && isSubsetOf(right_expr_source, left_mask))
    {
        join_condition.predicates.push_back(JoinPredicate{
            join_flatten.addExpression(right_node, right_expr_source),
            join_flatten.addExpression(left_node, left_expr_source),
            flipPredicateOperator(predicate_operator.value())});
        return true;
    }

    return false;
}

void buildJoinUsingCondition(const QueryTreeNodePtr & node, JoinFlattenContext & join_flatten, JoinCondition & join_condition)
{
    auto & using_list = node->as<ListNode &>();
    for (auto & using_node : using_list.getNodes())
    {
        auto & using_column_node = using_node->as<ColumnNode &>();
        auto & inner_columns_list = using_column_node.getExpressionOrThrow()->as<ListNode &>();
        chassert(inner_columns_list.getNodes().size() == 2);

        auto lhs_src = join_flatten.getExpressionSource(inner_columns_list.getNodes().at(0));
        auto rhs_src = join_flatten.getExpressionSource(inner_columns_list.getNodes().at(1));

        if (!isSubsetOf(lhs_src, join_flatten.getLeftMask()) || !isSubsetOf(rhs_src, join_flatten.getRightMask()))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "JOIN USING column {} does not belong to left or right table",
                inner_columns_list.formatASTForErrorMessage());

        join_condition.predicates.emplace_back(JoinPredicate{
            join_flatten.addExpression(inner_columns_list.getNodes().at(0), lhs_src),
            join_flatten.addExpression(inner_columns_list.getNodes().at(1), rhs_src),
            PredicateOperator::Equals});
    }

    /// For ASOF join, the last column in USING list is the ASOF column
    if (join_flatten.join_operator.strictness == JoinStrictness::Asof && !join_condition.predicates.empty())
        join_condition.predicates.back().op = PredicateOperator::GreaterOrEquals;
}

void buildJoinCondition(const QueryTreeNodePtr & node, JoinFlattenContext & join_flatten, JoinCondition & join_condition)
{
    std::string function_name;
    const auto * function_node = node->as<FunctionNode>();
    if (function_node)
        function_name = function_node->getFunction()->getName();

    if (function_name == "and")
    {
        for (const auto & child : function_node->getArguments())
            buildJoinCondition(child, join_flatten, join_condition);
        return;
    }

    bool is_predicate = tryGetJoinPredicate(function_node, join_flatten, join_condition);
    if (is_predicate)
        return;

    auto expr_source = join_flatten.getExpressionSource(node);
    join_condition.restrict_conditions.push_back(join_flatten.addExpression(node, expr_source));
}

void buildDisjunctiveJoinConditions(const QueryTreeNodePtr & node, JoinFlattenContext & join_flatten, std::vector<JoinCondition> & join_conditions)
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
            buildDisjunctiveJoinConditions(child, join_flatten, join_conditions);
        return;
    }
    buildJoinCondition(node, join_flatten, join_conditions.emplace_back());
}


void addConditionsToJoinOperator(JoinFlattenContext & join_flatten, std::vector<JoinCondition> join_conditions)
{
    if (!join_conditions.empty())
    {
        join_flatten.join_operator.expression.condition = std::move(join_conditions.back());
        join_conditions.pop_back();
        join_flatten.join_operator.expression.disjunctive_conditions = std::move(join_conditions);
    }
}


void buildDisjunctiveJoinConditions(const QueryTreeNodePtr & node, JoinFlattenContext & join_flatten)
{
    std::vector<JoinCondition> join_conditions;
    buildDisjunctiveJoinConditions(node, join_flatten, join_conditions);
    addConditionsToJoinOperator(join_flatten, std::move(join_conditions));
}

static bool hasEquiConditions(const JoinCondition & condition)
{
    for (const auto & predicate : condition.predicates)
    {
        if (predicate.op == PredicateOperator::Equals
         || predicate.op == PredicateOperator::NullSafeEquals)
            return true;
    }
    return false;
}


JoinCondition concatConditions(const JoinCondition & lhs, const JoinCondition & rhs)
{
    JoinCondition result = lhs;
    result.predicates.insert(result.predicates.end(), rhs.predicates.begin(), rhs.predicates.end());
    result.restrict_conditions.insert(result.restrict_conditions.end(), rhs.restrict_conditions.begin(), rhs.restrict_conditions.end());
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


void buildDisjunctiveJoinConditionsGeneral(const QueryTreeNodePtr & join_expression, JoinFlattenContext & join_flatten)
{
    using JoinConditions = std::vector<JoinCondition>;
    if (join_expression->getNodeType() != QueryTreeNodeType::FUNCTION)
    {
        std::vector<JoinCondition> join_conditions;
        buildJoinCondition(join_expression, join_flatten, join_conditions.emplace_back());
        addConditionsToJoinOperator(join_flatten, std::move(join_conditions));
        return;
    }

    std::unordered_map<const IQueryTreeNode *, std::vector<JoinCondition>> built_clauses;
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
        auto expression_source = join_flatten.getExpressionSource(node);
        bool references_both_sides = expression_source.test(join_flatten.getCurrentRelIndex()) && expression_source.size() > 1;

        // If the expression is a logical expression and it contains expressions from both sides, let's combine the clauses, otherwise let's just build one join clause
        bool is_complex = (function_name == "and" || function_name == "or") && references_both_sides;
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
                    buildJoinCondition(node, join_flatten, result.emplace_back());
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
            buildJoinCondition(node, join_flatten, clauses.emplace_back());
            built_clauses.emplace(node.get(), std::move(clauses));
        }
    }

    addConditionsToJoinOperator(join_flatten, std::move(built_clauses.at(join_expression.get())));
}

JoinTreeQueryPlan mergeJoinTreePlans(
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan)
{
    if (left_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns
        || right_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge join tree plans with different stages: {} and {}",
            left_join_tree_query_plan.from_stage, right_join_tree_query_plan.from_stage);
    }

    QueryPlan result_plan = std::move(left_join_tree_query_plan.query_plan);
    result_plan.unitePlans(std::move(right_join_tree_query_plan.query_plan));

    /// Collect all required row_policies and actions sets from left and right join tree query plans

    auto result_used_row_policies = std::move(left_join_tree_query_plan.used_row_policies);
    for (const auto & right_join_tree_query_plan_row_policy : right_join_tree_query_plan.used_row_policies)
        result_used_row_policies.insert(right_join_tree_query_plan_row_policy);

    auto result_useful_sets = std::move(left_join_tree_query_plan.useful_sets);
    for (const auto & useful_set : right_join_tree_query_plan.useful_sets)
        result_useful_sets.insert(useful_set);

    auto result_query_node_to_plan_step_mapping = std::move(left_join_tree_query_plan.query_node_to_plan_step_mapping);
    const auto & r_mapping = right_join_tree_query_plan.query_node_to_plan_step_mapping;
    result_query_node_to_plan_step_mapping.insert(r_mapping.begin(), r_mapping.end());


    return JoinTreeQueryPlan{
        .query_plan = std::move(result_plan),
        .from_stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(result_used_row_policies),
        .useful_sets = std::move(result_useful_sets),
        .query_node_to_plan_step_mapping = std::move(result_query_node_to_plan_step_mapping),
        .table_expression_to_join_input_mapping = std::move(left_join_tree_query_plan.table_expression_to_join_input_mapping),
    };
}

JoinTreeQueryPlan buildJoinStepLogical(
    JoinTreeQueryPlan left_plan,
    JoinTreeQueryPlan right_plan,
    const NameSet & outer_scope_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    auto query_context = planner_context->getQueryContext();


    auto * root_node = left_plan.query_plan.getRootNode();
    JoinStepLogical * join_step = root_node ? typeid_cast<JoinStepLogical *>(root_node->step.get()) : nullptr;

    bool can_flatten = join_step && join_step->canFlatten();
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: {} {}", __FILE__, __LINE__, can_flatten, join_node.formatASTForErrorMessage());
    if (!can_flatten)
        join_step = nullptr;

    auto & table_expression_to_join_input_mapping = left_plan.table_expression_to_join_input_mapping;
    if (!join_step)
    {
        table_expression_to_join_input_mapping.clear();
        for (const auto * table_expression : extractTableExpressionsSet(join_node.getLeftTableExpression()))
            table_expression_to_join_input_mapping.emplace(table_expression, 0);

        const auto & left_header = left_plan.query_plan.getCurrentHeader();

        const auto & settings = planner_context->getQueryContext()->getSettingsRef();
        auto join_step_holder = std::make_unique<JoinStepLogical>(
            left_header,
            settings[Setting::join_use_nulls],
            JoinSettings(settings),
            SortingStep::Settings(settings));
        join_step = join_step_holder.get();
        left_plan.query_plan.addStep(std::move(join_step_holder));
    }

    join_step->addRequiredOutput(outer_scope_columns);

    const auto & right_header = right_plan.query_plan.getCurrentHeader();
    auto & join_operator = join_step->addInput(
        {join_node.getKind(), join_node.getStrictness(), join_node.getLocality()},
        right_header);

    auto prepared_storage = tryGetStorageInTableJoin(join_node.getRightTableExpression(), planner_context);
    join_step->setPreparedJoinStorage(std::move(prepared_storage));

    for (const auto * table_expression : extractTableExpressionsSet(join_node.getRightTableExpression()))
        table_expression_to_join_input_mapping.emplace(table_expression, join_step->getNumberOfTables() - 1);

    JoinFlattenContext join_flatten(join_node, *join_step, join_operator, table_expression_to_join_input_mapping, planner_context);

    const auto & join_on_expression = join_node.getJoinExpression();
    auto join_expression_constant = tryExtractConstantFromConditionNode(join_on_expression);
    if (join_on_expression && join_on_expression->getNodeType() == QueryTreeNodeType::CONSTANT && !join_expression_constant.has_value())
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Wrong type {} of JOIN expression {}",
            join_on_expression->getResultType()->getName(), join_on_expression->formatASTForErrorMessage());
    }

    auto join_expression_node = getJoinExpressionFromNode(join_node);

    const auto & query_settings = join_flatten.planner_context->getQueryContext()->getSettingsRef();
    const auto & join_algorithms = query_settings[Setting::join_algorithm];

    /// CROSS/PASTE JOIN: doesn't have expression
    if (join_expression_node == nullptr)
    {
        if (!isCrossOrComma(join_node.getKind()) && !isPaste(join_node.getKind()))
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Missing join expression in {}", join_node.formatASTForErrorMessage());
    }
    /// USING
    else if (join_node.isUsingJoinExpression())
    {
        buildJoinUsingCondition(join_expression_node, join_flatten, join_flatten.join_operator.expression.condition);
        join_flatten.join_operator.expression.is_using = true;
    }
    /// JOIN ON non-constant expression
    else if (!join_expression_constant.has_value() || join_flatten.join_operator.strictness == JoinStrictness::Asof)
    {
        if (join_expression_node->getNodeType() != QueryTreeNodeType::FUNCTION)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression expected function",
                join_node.formatASTForErrorMessage());

        bool use_general_join_planning = (TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH)
            || TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::AUTO)) && query_settings[Setting::allow_general_join_planning];

        if (use_general_join_planning)
            buildDisjunctiveJoinConditionsGeneral(join_expression_node, join_flatten);
        else
            buildDisjunctiveJoinConditions(join_expression_node, join_flatten);
    }
    else if (join_expression_constant.has_value())
    {
        if (!TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JOIN ON constant supported only with join algorithm 'hash'");
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO");

        /*
        auto left_mask = join_flatten.getLeftMask();
        auto right_mask = join_flatten.getRightMask();
        auto & left_actions = join_flatten.getActions(left_mask);
        auto & right_actions = join_flatten.getActions(right_mask);

        /// Joined table expression always has __tableN prefix, other columns will appear only in projections, so these names are safe
        if (left_actions->tryFindInOutputs("__lhs_const"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected reserved name '__lhs_const' in JOIN expression {}", left_actions->dumpDAG());
        if (right_actions->tryFindInOutputs("__rhs_const"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected reserved name '__rhs_const' in JOIN expression {}", right_actions->dumpDAG());

        bool join_expression_value = join_expression_constant.value();
        auto dt = std::make_shared<DataTypeUInt8>();
        const auto & lhs_node = left_actions->addColumn(
            ColumnWithTypeAndName(dt->createColumnConstWithDefaultValue(1), dt, "__lhs_const"));
        left_actions->addOrReplaceInOutputs(lhs_node);

        const auto & rhs_node = right_actions->addColumn(
            ColumnWithTypeAndName(dt->createColumnConst(1, join_expression_value ? 0 : 1), dt, "__rhs_const"));
        right_actions->addOrReplaceInOutputs(rhs_node);

        JoinPredicate predicate = {
            JoinActionRef(&lhs_node, left_mask),
            JoinActionRef(&rhs_node, right_mask),
            PredicateOperator::Equals};

        join_flatten.join_operator.expression.condition.predicates.push_back(std::move(predicate));
        */
    }

    /// FIXME: add only newly created actions
    appendSetsFromActionsDAG(join_step->getExpressionActions(), left_plan.useful_sets);

    return mergeJoinTreePlans(std::move(left_plan), std::move(right_plan));
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
