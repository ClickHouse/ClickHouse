#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Common/JSONBuilder.h>
#include <Common/safe_cast.h>
#include <Common/typeid_cast.h>
#include "Core/ColumnWithTypeAndName.h"

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/isNotDistinctFrom.h>
#include <Functions/IsOperation.h>
#include <Functions/tuple.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PasteJoin.h>
#include <Interpreters/TableJoin.h>

#include <IO/Operators.h>

#include <Planner/PlannerJoins.h>

#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/JoiningTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/StorageJoin.h>

#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <algorithm>
#include <ranges>
#include <stack>
#include <unordered_map>

namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsUInt64 default_max_bytes_in_join;
    extern const SettingsBool join_use_nulls;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int INCORRECT_DATA;
}

std::optional<ASOFJoinInequality> operatorToAsofInequality(JoinConditionOperator op)
{
    switch (op)
    {
        case JoinConditionOperator::Less: return ASOFJoinInequality::Less;
        case JoinConditionOperator::LessOrEquals: return ASOFJoinInequality::LessOrEquals;
        case JoinConditionOperator::Greater: return ASOFJoinInequality::Greater;
        case JoinConditionOperator::GreaterOrEquals: return ASOFJoinInequality::GreaterOrEquals;
        default: return {};
    }
}

static void addToNullableIfNeeded(
    JoinExpressionActions & expression_actions,
    JoinKind join_kind,
    bool use_nulls,
    const NameSet & required_output_columns,
    std::vector<const ActionsDAG::Node *> & actions_after_join,
    const std::unordered_map<String, const ActionsDAG::Node *> & changed_types)
{
    auto to_nullable = FunctionFactory::instance().get("toNullable", nullptr);

    auto actions_dag = expression_actions.getActionsDAG();
    auto & outputs = actions_dag->getOutputs();
    outputs.clear();

    bool to_null_left = use_nulls && isRightOrFull(join_kind);
    bool to_null_right = use_nulls && isLeftOrFull(join_kind);
    for (const auto * node : actions_dag->getInputs())
    {
        if (!required_output_columns.contains(node->result_name))
            continue;

        String original_name = node->result_name;

        if (auto it = changed_types.find(node->result_name); it != changed_types.end())
        {
            node = it->second;
        }

        JoinActionRef input_action(node, expression_actions);
        bool convert_to_nullable = (to_null_left && input_action.fromLeft()) || (to_null_right && input_action.fromRight());
        if (convert_to_nullable && removeLowCardinality(node->result_type)->canBeInsideNullable())
        {
            node = &actions_dag->addFunction(to_nullable, {node}, {});
        }

        if (node->result_name != original_name)
            node = &actions_dag->addAlias(*node, std::move(original_name));

        outputs.push_back(node);
        actions_after_join.push_back(node);
    }

    if (outputs.empty())
    {
        ColumnWithTypeAndName column(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "dummy");
        const auto * node = &actions_dag->addColumn(std::move(column));
        actions_after_join.push_back(node);
        outputs.push_back(node);
    }
}

JoinStepLogical::JoinStepLogical(
    const Block & left_header_,
    const Block & right_header_,
    JoinOperator join_operator_,
    JoinExpressionActions join_expression_actions_,
    const NameSet & required_output_columns_,
    const std::unordered_map<String, const ActionsDAG::Node *> & changed_types,
    bool use_nulls_,
    JoinSettings join_settings_,
    SortingStep::Settings sorting_settings_)
    : expression_actions(std::move(join_expression_actions_))
    , join_operator(std::move(join_operator_))
    , use_nulls(use_nulls_)
    , join_settings(std::move(join_settings_))
    , sorting_settings(std::move(sorting_settings_))
{
    addToNullableIfNeeded(expression_actions, join_operator.kind, use_nulls_, required_output_columns_, actions_after_join, changed_types);
    updateInputHeaders({left_header_, right_header_});
}

JoinStepLogical::JoinStepLogical(
    const Block & left_header_,
    const Block & right_header_,
    JoinOperator join_operator_,
    JoinExpressionActions join_expression_actions_,
    std::vector<const ActionsDAG::Node *> actions_after_join_,
    JoinSettings join_settings_,
    SortingStep::Settings sorting_settings_)
    : expression_actions(std::move(join_expression_actions_))
    , join_operator(std::move(join_operator_))
    , join_settings(std::move(join_settings_))
    , sorting_settings(std::move(sorting_settings_))
{
    actions_after_join = std::move(actions_after_join_);
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr JoinStepLogical::updatePipeline(QueryPipelineBuilders /* pipelines */, const BuildQueryPipelineSettings & /* settings */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot execute JoinStepLogical, it should be converted physical step first");
}

void JoinStepLogical::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

String formatJoinCondition(const std::vector<JoinActionRef> & predicates)
{
    return fmt::format("{}", fmt::join(predicates | std::views::transform([](const auto & x) { return x.getColumnName(); }), " AND "));
}

std::vector<std::pair<String, String>> JoinStepLogical::describeJoinProperties() const
{
    std::vector<std::pair<String, String>> description;

    description.emplace_back("Type", toString(join_operator.kind));
    description.emplace_back("Strictness", toString(join_operator.strictness));
    description.emplace_back("Locality", toString(join_operator.locality));
    description.emplace_back("Expression", formatJoinCondition(join_operator.expression));

    for (const auto & [name, value] : runtime_info_description)
        description.emplace_back(name, value);

    return description;
}

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);

    for (const auto & [name, value] : describeJoinProperties())
        settings.out << prefix << name << ": " << value << '\n';

    settings.out << prefix << "Expression:\n";
    auto actions_dag = expression_actions.getActionsDAG()->clone();
    ExpressionActions(std::move(actions_dag)).describeActions(settings.out, prefix);
}

void JoinStepLogical::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinProperties())
        map.add(name, value);

    auto actions_dag = expression_actions.getActionsDAG()->clone();
    map.add("Actions", ExpressionActions(std::move(actions_dag)).toTree());
}

void JoinStepLogical::updateOutputHeader()
{
    auto actions_dag = expression_actions.getActionsDAG();
    Header & header = output_header.emplace(actions_dag->getResultColumns());

    for (auto & column : header)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    if (!header.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is empty, actions_dag: {}", actions_dag->dumpDAG());
}

JoinStepLogicalLookup::JoinStepLogicalLookup(Block header, PreparedJoinStorage prepared_join_storage_)
    : ISourceStep(std::move(header))
    , prepared_join_storage(std::move(prepared_join_storage_))
{
}

void JoinStepLogicalLookup::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot read from {} step. This step should be optimized to FilledJoin step.", getName());
}

/// When we have an expression like `a AND b`, it can work even when `a` and `b` are non-boolean values,
/// because the AND operator will implicitly convert them to booleans. The result will be either boolean or nullable.
/// In some cases we need to split `a` and `b` into separate expressions, but we want to preserve the same
/// boolean conversion behavior as if they were still part of the original AND expression.
JoinActionRef toBoolIfNeeded(JoinActionRef condition)
{
    auto output_type = removeNullable(condition.getType());
    WhichDataType which_type(output_type);
    if (!which_type.isUInt8())
        return JoinActionRef::transform({condition}, [](auto & dag, auto && nodes) { return &dag.addCast(*nodes.at(0), std::make_shared<DataTypeUInt8>(), {}); });
    return condition;
}

bool canPushDownFromOn(const JoinOperator & join_operator, std::optional<JoinTableSide> side = {})
{
    bool is_suitable_kind = join_operator.kind == JoinKind::Inner
        || join_operator.kind == JoinKind::Cross
        || join_operator.kind == JoinKind::Comma
        || join_operator.kind == JoinKind::Paste
        || (side == JoinTableSide::Left && join_operator.kind == JoinKind::Right)
        || (side == JoinTableSide::Right && join_operator.kind == JoinKind::Left);

    return is_suitable_kind && join_operator.strictness == JoinStrictness::All;
}

void predicateOperandsToCommonType(JoinActionRef & left_node, JoinActionRef & right_node)
{
    const auto & left_type = left_node.getType();
    const auto & right_type = right_node.getType();

    if (left_type->equals(*right_type))
        return;

    DataTypePtr common_type;
    try
    {
        common_type = getLeastSupertype(DataTypes{left_type, right_type});
    }
    catch (Exception & ex)
    {
        ex.addMessage("JOIN cannot infer common type in ON section for keys. Left key '{}' type {}. Right key '{}' type {}",
            left_node.getColumnName(), left_type->getName(),
            right_node.getColumnName(), right_type->getName());
        throw;
    }

    auto cast_transform = [&common_type](auto & dag, auto && nodes) { return &dag.addCast(*nodes.at(0), common_type, {}); };
    if (!left_type->equals(*common_type))
        left_node = JoinActionRef::transform({left_node}, cast_transform);

    if (!right_type->equals(*common_type))
        right_node = JoinActionRef::transform({right_node}, cast_transform);
}

bool addJoinPredicatesToTableJoin(std::vector<JoinActionRef> & predicates, TableJoin::JoinOnClause & table_join_clause, std::unordered_set<JoinActionRef> & used_expressions)
{
    // std::cerr << "addJoinPredicatesToTableJoin" << std::endl;
    bool has_join_predicates = false;
    std::vector<JoinActionRef> new_predicates;
    for (auto & pred : predicates)
    {
        auto & predicate = new_predicates.emplace_back(std::move(pred));
        // std::cerr << "Predicate: " << predicate.getColumnName() << std::endl;
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        predicateOperandsToCommonType(lhs, rhs);
        bool null_safe_comparison = JoinConditionOperator::NullSafeEquals == predicate_op;
        if (null_safe_comparison && isNullableOrLowCardinalityNullable(lhs.getType()) && isNullableOrLowCardinalityNullable(rhs.getType()))
        {
            /**
                * In case of null-safe comparison (a IS NOT DISTINCT FROM b),
                * we need to wrap keys with a non-nullable type.
                * The type `tuple` can be used for this purpose,
                * because value tuple(NULL) is not NULL itself (moreover it has type Tuple(Nullable(T) which is not Nullable).
                * Thus, join algorithm will match keys with values tuple(NULL).
                * Example:
                *   SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.b
                * This will be semantically transformed to:
                *   SELECT * FROM t1 JOIN t2 ON tuple(t1.a) == tuple(t2.b)
                */

            JoinActionRef::AddFunction wrap_nullsafe_function(std::make_shared<FunctionTuple>());
            lhs = JoinActionRef::transform({lhs}, wrap_nullsafe_function);
            rhs = JoinActionRef::transform({rhs}, wrap_nullsafe_function);
        }

        has_join_predicates = true;
        // std::cerr << "Adding key " << lhs.getColumnName() << ' ' << rhs.getColumnName() << std::endl;
        table_join_clause.addKey(lhs.getColumnName(), rhs.getColumnName(), null_safe_comparison);

        /// We applied predicate, do not add it to residual conditions
        used_expressions.insert(lhs);
        used_expressions.insert(rhs);
        new_predicates.pop_back();
    }

    predicates = std::move(new_predicates);

    return has_join_predicates;
}


static Block blockWithColumns(ColumnsWithTypeAndName columns)
{
    Block block;
    for (const auto & column : columns)
        block.insert(ColumnWithTypeAndName(column.column ? column.column : column.type->createColumn(), column.type, column.name));
    return block;
}

using QueryPlanNode = QueryPlan::Node;
using QueryPlanNodePtr = QueryPlanNode *;

JoinActionRef concatConditions(std::vector<JoinActionRef> & conditions, std::optional<JoinTableSide> side = {})
{
    auto matching_point = std::ranges::partition(conditions,
        [side](const auto & node)
        {
            if (side == JoinTableSide::Left)
                return node.fromLeft() || node.fromNone();
            else if (side == JoinTableSide::Right)
                return node.fromRight();
            else
                return true;
        });

    std::vector<JoinActionRef> matching(conditions.begin(), matching_point.begin());
    JoinActionRef result(nullptr);
    if (matching.size() == 1)
        result = toBoolIfNeeded(matching.front());
    else if (matching.size() > 1)
        result = JoinActionRef::transform({matching}, JoinActionRef::AddFunction(JoinConditionOperator::And));

    conditions.erase(conditions.begin(), matching_point.begin());
    return result;
}

bool tryAddDisjunctiveConditions(
    std::vector<JoinActionRef> & join_expressions,
    TableJoin::Clauses & table_join_clauses,
    std::unordered_set<JoinActionRef> & used_expressions,
    bool throw_on_error = false)
{
    if (join_expressions.size() != 1)
        return false;

    auto & join_expression = join_expressions.front();
    if (!join_expression.isFunction(JoinConditionOperator::Or))
        return false;

    std::vector<JoinActionRef> disjunctive_conditions = join_expression.getArguments();
    bool has_residual_condition = false;
    for (const auto & expr : disjunctive_conditions)
    {
        std::vector<JoinActionRef> join_condition = {expr};
        if (expr.isFunction(JoinConditionOperator::And))
            join_condition = expr.getArguments();

        auto & table_join_clause = table_join_clauses.emplace_back();
        bool has_keys = addJoinPredicatesToTableJoin(join_condition, table_join_clause, used_expressions);
        if (!has_keys && throw_on_error)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                formatJoinCondition({expr}));
        if (!has_keys && !throw_on_error)
            return false;

        if (auto left_pre_filter_condition = concatConditions(join_condition, JoinTableSide::Left))
        {
            table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
            used_expressions.insert(left_pre_filter_condition);
        }

        if (auto right_pre_filter_condition = concatConditions(join_condition, JoinTableSide::Right))
        {
            table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
            used_expressions.insert(right_pre_filter_condition);
        }

        if (!join_condition.empty())
            has_residual_condition = true;
    }

    /// Clear join_expressions if there is no unhandled conditions, no need to calculate residual filter
    if (!has_residual_condition)
        join_expressions.clear();

    return true;
}

static void addSortingForMergeJoin(
    const FullSortingMergeJoin * join_ptr,
    QueryPlan::Node *& left_node,
    QueryPlan::Node *& right_node,
    QueryPlan::Nodes & nodes,
    const SortingStep::Settings & sort_settings,
    const JoinSettings & join_settings,
    const TableJoin & table_join)
{
    auto join_kind = table_join.kind();
    auto join_strictness = table_join.strictness();
    auto add_sorting = [&] (QueryPlan::Node *& node, const Names & key_names, JoinTableSide join_table_side)
    {
        SortDescription sort_description;
        sort_description.reserve(key_names.size());
        for (const auto & key_name : key_names)
            sort_description.emplace_back(key_name);

        auto sorting_step = std::make_unique<SortingStep>(
            node->step->getOutputHeader(), std::move(sort_description), 0 /*limit*/, sort_settings, true /*is_sorting_for_merge_join*/);
        sorting_step->setStepDescription(fmt::format("Sort {} before JOIN", join_table_side));
        node = &nodes.emplace_back(QueryPlan::Node{std::move(sorting_step), {node}});
    };

    auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
    auto add_create_set = [&](QueryPlan::Node *& node, const Names & key_names, JoinTableSide join_table_side)
    {
        auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
            node->step->getOutputHeader(), key_names, join_settings.max_rows_in_set_to_optimize_join, crosswise_connection, join_table_side);
        creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_table_side));

        auto * step_raw_ptr = creating_set_step.get();
        node = &nodes.emplace_back(QueryPlan::Node{std::move(creating_set_step), {node}});
        return step_raw_ptr;
    };

    const auto & join_clause = join_ptr->getTableJoin().getOnlyClause();

    bool join_type_allows_filtering = (join_strictness == JoinStrictness::All || join_strictness == JoinStrictness::Any)
                                    && (isInner(join_kind) || isLeft(join_kind) || isRight(join_kind));

    auto has_non_const = [](const Block & block, const auto & keys)
    {
        for (const auto & key : keys)
        {
            const auto & column = block.getByName(key).column;
            if (column && !isColumnConst(*column))
                return true;
        }
        return false;
    };

    /// This optimization relies on the sorting that should buffer data from both streams before emitting any rows.
    /// Sorting on a stream with const keys can start returning rows immediately and pipeline may stuck.
    /// Note: it's also doesn't work with the read-in-order optimization.
    /// No checks here because read in order is not applied if we have `CreateSetAndFilterOnTheFlyStep` in the pipeline between the reading and sorting steps.
    bool has_non_const_keys = has_non_const(left_node->step->getOutputHeader(), join_clause.key_names_left)
        && has_non_const(right_node->step->getOutputHeader() , join_clause.key_names_right);
    if (join_settings.max_rows_in_set_to_optimize_join > 0 && join_type_allows_filtering && has_non_const_keys)
    {
        auto * left_set = add_create_set(left_node, join_clause.key_names_left, JoinTableSide::Left);
        auto * right_set = add_create_set(right_node, join_clause.key_names_right, JoinTableSide::Right);

        if (isInnerOrLeft(join_kind))
            right_set->setFiltering(left_set->getSet());

        if (isInnerOrRight(join_kind))
            left_set->setFiltering(right_set->getSet());
    }

    add_sorting(left_node, join_clause.key_names_left, JoinTableSide::Left);
    add_sorting(right_node, join_clause.key_names_right, JoinTableSide::Right);
}


static void constructPhysicalStep(
    QueryPlanNode & node,
    ActionsDAG left_pre_join_actions,
    ActionsDAG post_join_actions,
    std::pair<String, bool> residual_filter_condition,
    JoinPtr join_ptr,
    const QueryPlanOptimizationSettings & ,
    const JoinSettings & join_settings,
    const SortingStep::Settings &,
    QueryPlan::Nodes & nodes)
{
    if (!join_ptr->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join is not filled");

    node.children.resize(1);

    // std::cerr << "constructPhysicalStep" << std::endl;
    auto * join_left_node = node.children[0];
    // std::cerr << join_left_node->step->getOutputHeader().dumpStructure() << std::endl;
    makeExpressionNodeOnTopOf(*join_left_node, std::move(left_pre_join_actions), {}, nodes, "Left Join Actions");

    // std::cerr << join_left_node->step->getOutputHeader().dumpStructure() << std::endl;
    // std::cerr << left_pre_join_actions.dumpDAG() << std::endl;
    node.step = std::make_unique<FilledJoinStep>(
        join_left_node->step->getOutputHeader(),
        join_ptr,
        join_settings.max_block_size);
    // std::cerr << node.step->getOutputHeader().dumpStructure() << std::endl;
    // std::cerr << post_join_actions.dumpDAG() << std::endl;
    makeExpressionNodeOnTopOf(node, std::move(post_join_actions), residual_filter_condition.first, nodes, "Post Join Actions");
}

static void constructPhysicalStep(
    QueryPlanNode & node,
    ActionsDAG left_pre_join_actions,
    ActionsDAG right_pre_join_actions,
    ActionsDAG post_join_actions,
    std::pair<String, bool> residual_filter_condition,
    JoinPtr join_ptr,
    const QueryPlanOptimizationSettings & optimization_settings,
    const JoinSettings & join_settings,
    const SortingStep::Settings & sorting_settings,
    QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 2 children, got {}", node.children.size());

    if (join_ptr->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join is already filled");

    auto * join_left_node = node.children[0];
    auto * join_right_node = node.children[1];

    makeExpressionNodeOnTopOf(*join_left_node, std::move(left_pre_join_actions), {}, nodes, "Left Pre Join Actions");
    makeExpressionNodeOnTopOf(*join_right_node, std::move(right_pre_join_actions), {}, nodes, "Right Pre Join Actions");

    if (const auto * fsmjoin = dynamic_cast<const FullSortingMergeJoin *>(join_ptr.get()))
        addSortingForMergeJoin(fsmjoin, join_left_node, join_right_node, nodes,
            sorting_settings, join_settings, fsmjoin->getTableJoin());

    auto required_output_from_join = post_join_actions.getRequiredColumnsNames();
    node.step = std::make_unique<JoinStep>(
        join_left_node->step->getOutputHeader(),
        join_right_node->step->getOutputHeader(),
        join_ptr,
        join_settings.max_block_size,
        join_settings.min_joined_block_size_bytes,
        optimization_settings.max_threads,
        NameSet(required_output_from_join.begin(), required_output_from_join.end()),
        false /*optimize_read_in_order*/,
        true /*use_new_analyzer*/);

    node.children = {join_left_node, join_right_node};

    makeExpressionNodeOnTopOf(node, std::move(post_join_actions), residual_filter_condition.first, nodes, "Post Join Actions");
}

static QueryPlanNode buildPhysicalJoinImpl(
    std::vector<QueryPlanNode *> children,
    JoinOperator join_operator,
    JoinExpressionActions expression_actions,
    bool use_nulls,
    JoinSettings join_settings,
    JoinAlgorithmParams join_algorithm_params,
    SortingStep::Settings sorting_settings,
    const ActionsDAG::NodeRawConstPtrs & actions_after_join,
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan::Nodes & nodes)
{
    auto * logical_lookup = typeid_cast<JoinStepLogicalLookup *>(children.back()->step.get());

    auto table_join = std::make_shared<TableJoin>(join_settings, use_nulls && logical_lookup,
        Context::getGlobalContextInstance()->getGlobalTemporaryVolume(),
        Context::getGlobalContextInstance()->getTempDataOnDisk());

    PreparedJoinStorage prepared_join_storage;
    if (logical_lookup)
    {
        prepared_join_storage = std::move(logical_lookup->getPreparedJoinStorage());
        prepared_join_storage.visit([&table_join](const auto & storage_)
        {
            table_join->setStorageJoin(storage_);
        });
    }

    auto & join_expression = join_operator.expression;

    std::unordered_set<JoinActionRef> used_expressions;

    bool is_disjunctive_condition = false;
    auto & table_join_clauses = table_join->getClauses();
    if (!isCrossOrComma(join_operator.kind) && !isPaste(join_operator.kind))
    {
        bool has_keys = addJoinPredicatesToTableJoin(join_expression, table_join_clauses.emplace_back(), used_expressions);

        if (!has_keys)
        {
            bool can_convert_to_cross = (isInner(join_operator.kind) || isCrossOrComma(join_operator.kind))
                && TableJoin::isEnabledAlgorithm(join_settings.join_algorithms, JoinAlgorithm::HASH)
                && join_operator.strictness == JoinStrictness::All;

            table_join_clauses.pop_back();
            is_disjunctive_condition = tryAddDisjunctiveConditions(
                join_expression, table_join_clauses, used_expressions, !can_convert_to_cross);

            if (!is_disjunctive_condition)
            {
                if (!can_convert_to_cross)
                    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                        formatJoinCondition(join_expression));

                join_operator.kind = JoinKind::Cross;
            }
        }
    }
    else if (!join_expression.empty())
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Unexpected JOIN ON expression {} for {} JOIN",
            formatJoinCondition(join_expression), toString(join_operator.kind));
    }

    if (join_operator.strictness == JoinStrictness::Asof)
    {
        if (is_disjunctive_condition)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple disjuncts in JOIN ON expression");

        /// Find strictly only one inequality in predicate list for ASOF join
        chassert(table_join_clauses.size() == 1);
        auto found_asof_predicate_it = join_expression.end();
        for (auto it = join_expression.begin(); it != join_expression.end(); ++it)
        {
            auto [predicate_op, lhs, rhs] = it->asBinaryPredicate();
            auto asof_inequality_op = operatorToAsofInequality(predicate_op);
            if (!asof_inequality_op)
                continue;

            if (lhs.fromRight() && rhs.fromLeft())
            {
                *asof_inequality_op = reverseASOFJoinInequality(*asof_inequality_op);
                std::swap(lhs, rhs);
            }
            else if (!lhs.fromLeft() || !rhs.fromRight())
                continue;

            if (found_asof_predicate_it != join_expression.end())
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple inequality predicates in JOIN ON expression");
            found_asof_predicate_it = it;

            predicateOperandsToCommonType(lhs, rhs);

            used_expressions.insert(lhs);
            used_expressions.insert(rhs);

            table_join->setAsofInequality(*asof_inequality_op);
            table_join_clauses.front().addKey(lhs.getColumnName(), rhs.getColumnName(), /* null_safe_comparison = */ false);
        }
        if (found_asof_predicate_it == join_expression.end())
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join requires one inequality predicate in JOIN ON expression, in {}",
                formatJoinCondition(join_expression));

        join_expression.erase(found_asof_predicate_it);
    }

    if (auto left_pre_filter_condition = concatConditions(join_expression, JoinTableSide::Left))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
        used_expressions.insert(left_pre_filter_condition);
    }

    if (auto right_pre_filter_condition = concatConditions(join_expression, JoinTableSide::Right))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
        used_expressions.insert(right_pre_filter_condition);
    }

    join_operator.residual_filter.append_range(join_expression);
    JoinActionRef residual_filter_condition = concatConditions(join_operator.residual_filter);
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> actions_after_join_map;
    for (const auto * action : actions_after_join)
    {
        if (action->type == ActionsDAG::ActionType::ALIAS)
        {
            //bool remove_right_nullable = prepared_join_storage && use_nulls && isLeftOrFull(join_operator.kind);
            if (prepared_join_storage && JoinActionRef(action, expression_actions).fromRight())
            {
                /// StorageJoin should convert to nullable by itself.
            }
            else
            {
                /// x (Alias) -> toNullable(x) -> x (Input)
                action = action->children.at(0);
                /// toNullable(x) -> x (Input)
            }
        }

        actions_after_join_map[action] = action;
    }

    std::vector<const ActionsDAG::Node *> required_residual_nodes;
    if (residual_filter_condition)
    {
        std::stack<const ActionsDAG::Node *> stack;
        stack.push(residual_filter_condition.getNode());
        while (!stack.empty())
        {
            const auto * node = stack.top();
            stack.pop();
            if (node->type == ActionsDAG::ActionType::INPUT)
            {
                if (actions_after_join_map.contains(node))
                    continue;
                actions_after_join_map[node] = node;
                required_residual_nodes.push_back(node);
            }
            for (const auto * child : node->children)
                stack.push(child);
        }
    }

    if (residual_filter_condition && (is_disjunctive_condition || !canPushDownFromOn(join_operator)))
    {
        auto residual_filter_dag = JoinExpressionActions::getSubDAG(std::views::single(residual_filter_condition));
        ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
        mixed_join_expression = std::make_shared<ExpressionActions>(std::move(residual_filter_dag), optimization_settings.actions_settings);
        residual_filter_condition = JoinActionRef(nullptr);
    }

    for (const auto * action : actions_after_join)
    {
        //bool remove_right_nullable = prepared_join_storage && use_nulls && isLeftOrFull(join_operator.kind);
        if (action->type == ActionsDAG::ActionType::ALIAS)
        {
            if (prepared_join_storage && JoinActionRef(action, expression_actions).fromRight())
            {
                /// x (Alias) -> toNullable(x) -> x (Input)
                action = action->children.at(0)->children.at(0);
                /// toNullable(x) -> x (Input)
            }
            else
                action = action->children.at(0);
        }

        used_expressions.emplace(action, expression_actions);
    }
    for (const auto * action : required_residual_nodes)
        used_expressions.emplace(action, expression_actions);

    // std::cerr << expression_actions.getActionsDAG()->dumpDAG() << std::endl;

    ActionsDAG left_dag = JoinExpressionActions::getSubDAG(used_expressions | std::views::filter([](const auto & node) { return node.fromLeft() || node.fromNone(); }));
    ActionsDAG right_dag = JoinExpressionActions::getSubDAG(used_expressions | std::views::filter([](const auto & node) { return node.fromRight(); }));

    // std::cerr << left_dag.dumpDAG() << std::endl;
    // std::cerr << right_dag.dumpDAG() << std::endl;

    auto common_dag = expression_actions.getActionsDAG();

    bool can_remove_residual_filter = false;
    auto & required_output_nodes = common_dag->getOutputs();
    if (residual_filter_condition && !std::ranges::contains(required_output_nodes, residual_filter_condition.getNode()))
    {
        can_remove_residual_filter = true;
        required_output_nodes.emplace_back(residual_filter_condition.getNode());
    }

    ActionsDAG residual_dag = ActionsDAG::foldActionsByProjection(actions_after_join_map, required_output_nodes);

    // std::cerr << "Residual \n" << residual_dag.dumpDAG() << std::endl;

    table_join->setInputColumns(
        left_dag.getNamesAndTypesList(),
        right_dag.getNamesAndTypesList());
    table_join->setUsedColumns(residual_dag.getRequiredColumnsNames());
    table_join->setJoinOperator(join_operator);

    Block left_sample_block = blockWithColumns(left_dag.getResultColumns());
    Block right_sample_block = blockWithColumns(right_dag.getResultColumns());

    // std::cerr << left_sample_block.dumpStructure() << std::endl;
    // std::cerr << right_sample_block.dumpStructure() << std::endl;
    // std::cerr << table_join->isJoinWithConstant() << std::endl;

    auto join_algorithm_ptr = chooseJoinAlgorithm(
        table_join,
        prepared_join_storage,
        left_sample_block,
        right_sample_block,
        join_algorithm_params);

    QueryPlanNode node;
    node.children = std::move(children);
    String residual_filter_condition_name = residual_filter_condition ? residual_filter_condition.getColumnName() : "";
    if (!join_algorithm_ptr->isFilled())
    {
        constructPhysicalStep(
            node, std::move(left_dag), std::move(right_dag), std::move(residual_dag), std::make_pair(residual_filter_condition_name, can_remove_residual_filter),
            std::move(join_algorithm_ptr), optimization_settings, join_settings, sorting_settings, nodes);
    }
    else
    {
        constructPhysicalStep(
            node, std::move(left_dag), std::move(residual_dag), std::make_pair(residual_filter_condition_name, can_remove_residual_filter),
            std::move(join_algorithm_ptr), optimization_settings, join_settings, sorting_settings, nodes);
    }
    return node;
}

void JoinStepLogical::buildPhysicalJoin(
    QueryPlanNode & node,
    std::vector<RelationStats> relation_stats,
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan::Nodes & nodes)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
    {
        if (node.step)
        {
            const auto & step = *node.step;
            auto type_name = demangle(typeid(step).name());
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected JoinStepLogical, got '{}' of type {}", node.step->getName(), type_name);
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected JoinStepLogical, got nullptr");
    }

    UInt64 hash_table_key_hash = 0;
    if (join_step->hash_table_key_hashes)
        hash_table_key_hash = join_step->hash_table_key_hashes->key_hash_left;

    JoinAlgorithmParams join_algorithm_params(
        join_step->join_settings,
        optimization_settings.max_threads,
        hash_table_key_hash,
        optimization_settings.max_entries_for_hash_table_stats,
        optimization_settings.initial_query_id,
        optimization_settings.lock_acquire_timeout);

    if (relation_stats.size() == 2 && relation_stats[1].estimated_rows > 0)
        join_algorithm_params.rhs_size_estimation = relation_stats[1].estimated_rows;

    auto new_node = buildPhysicalJoinImpl(
        node.children,
        join_step->join_operator,
        std::move(join_step->expression_actions),
        join_step->use_nulls,
        join_step->join_settings,
        join_algorithm_params,
        join_step->sorting_settings,
        join_step->actions_after_join,
        optimization_settings,
        nodes);

    node = std::move(new_node);
}

std::optional<ActionsDAG> JoinStepLogical::getFilterActions(JoinTableSide side, String & filter_column_name)
{
    if (join_operator.strictness != JoinStrictness::All)
        return {};

    auto & join_expression = join_operator.expression;

    if (!canPushDownFromOn(join_operator, side))
        return {};

    if (auto filter_condition = concatConditions(join_expression, side))
    {
        // TODO: try use `createActionsForConjunction`
        filter_column_name = filter_condition.getColumnName();
        ActionsDAG new_dag = JoinExpressionActions::getSubDAG(filter_condition);
        if (new_dag.getOutputs().size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 output column, got {}", new_dag.getOutputs().size());

        const auto & inputs = new_dag.getInputs();
        auto & outputs = new_dag.getOutputs();
        if (std::ranges::contains(inputs, outputs.front()))
            outputs.clear();
        outputs.append_range(inputs);

        return std::move(new_dag);
    }

    return {};
}

void JoinStepLogical::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    join_settings.updatePlanSettings(settings);
    sorting_settings.updatePlanSettings(settings);
}

void JoinStepLogical::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    writeIntBinary(flags, ctx.out);

    writeVarUInt(1, ctx.out);
    auto actions_dag = expression_actions.getActionsDAG();
    actions_dag->serialize(ctx.out, ctx.registry);

    join_operator.serialize(ctx.out, actions_dag.get());

    ActionsDAG::serializeNodeList(ctx.out, actions_dag->getNodeToIdMap(), actions_after_join);
}

std::unique_ptr<IQueryPlanStep> JoinStepLogical::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical must have two input streams");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    ActionsDAG actions_dag;
    {
        UInt64 num_dags;
        readVarUInt(num_dags, ctx.in);

        if (num_dags != 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical deserialization expect 3 DAGs, got {}", num_dags);

        actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    }

    auto left_header = ctx.input_headers.front();
    auto right_header = ctx.input_headers.back();
    auto id_to_node_map = actions_dag.getIdToNodeMap();
    JoinExpressionActions expression_actions(left_header, right_header, std::move(actions_dag));

    auto join_operator = JoinOperator::deserialize(ctx.in, expression_actions);

    auto actions_after_join = ActionsDAG::deserializeNodeList(ctx.in, id_to_node_map);

    SortingStep::Settings sort_settings(ctx.settings);
    JoinSettings join_settings(ctx.settings);

    return std::make_unique<JoinStepLogical>(
        std::move(left_header),
        std::move(right_header),
        std::move(join_operator),
        std::move(expression_actions),
        std::move(actions_after_join),
        std::move(join_settings),
        std::move(sort_settings));
}

QueryPlanStepPtr JoinStepLogical::clone() const
{
    auto new_join_operator = join_operator;
    auto new_expression_actions = expression_actions.clone(new_join_operator.expression);

    auto result_step = std::make_unique<JoinStepLogical>(
        getInputHeaders().front(), getInputHeaders().back(),
        std::move(new_join_operator),
        std::move(new_expression_actions),
        actions_after_join,
        join_settings,
        sorting_settings);
    result_step->setStepDescription(getStepDescription());
    return result_step;
}

void JoinStepLogical::addConditions(ActionsDAG::NodeRawConstPtrs conditions)
{
    for (const auto * node : conditions)
        join_operator.expression.emplace_back(node, expression_actions);
}

void registerJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Join", JoinStepLogical::deserialize);
}


}
