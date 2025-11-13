#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Common/JSONBuilder.h>
#include <Common/safe_cast.h>
#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Core/ColumnWithTypeAndName.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDynamic.h>

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
#include <Interpreters/HashTablesStatistics.h>

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
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/StorageJoin.h>

#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <algorithm>
#include <memory>
#include <ranges>
#include <stack>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsUInt64 default_max_bytes_in_join;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
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
        if (!required_output_columns.contains(node->result_name) && node->result_name != "__join_result_dummy")
            continue;

        String original_name = node->result_name;

        /// The `changed_types` map is used to handle type changes occurred in `USING` clause
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

        actions_after_join.push_back(node);
        outputs.push_back(node);
    }

    if (outputs.empty())
    {
        auto column_type = std::make_shared<DataTypeUInt8>();
        ColumnWithTypeAndName column(column_type->createColumnConst(1, 0), column_type, "__join_result_dummy");
        const auto * node = &actions_dag->addColumn(std::move(column));
        actions_after_join.push_back(node);
        outputs.push_back(node);
    }
}

JoinStepLogical::JoinStepLogical(
    SharedHeader left_header_,
    SharedHeader right_header_,
    JoinOperator join_operator_,
    JoinExpressionActions join_expression_actions_,
    const NameSet & required_output_columns_,
    const std::unordered_map<String, const ActionsDAG::Node *> & changed_types,
    bool use_nulls_,
    JoinSettings join_settings_,
    SortingStep::Settings sorting_settings_)
    : expression_actions(std::move(join_expression_actions_))
    , join_operator(std::move(join_operator_))
    , join_settings(std::move(join_settings_))
    , sorting_settings(std::move(sorting_settings_))
{
    if (!changed_types.empty())
    {
        /// FIXME: do not reorder join with `using` clause
        optimized = true;
    }
    addToNullableIfNeeded(expression_actions, join_operator.kind, use_nulls_, required_output_columns_, actions_after_join, changed_types);
    updateInputHeaders({left_header_, right_header_});
}

JoinStepLogical::JoinStepLogical(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
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

#ifndef NDEBUG
    auto all_nodes_set = std::ranges::to<std::unordered_set>(
        expression_actions.getActionsDAG()->getNodes() | std::views::transform([](const auto & node) { return &node; }));
    for (const auto * node_after_join : actions_after_join)
    {
        if (!all_nodes_set.contains(node_after_join))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} is not in the expression actions dag", fmt::ptr(node_after_join));
    }
#endif
}

std::unordered_set<JoinTableSide> JoinStepLogical::typeChangingSides() const
{
    std::unordered_set<JoinTableSide> result;
    for (const auto * node_after_join : actions_after_join)
    {
        if (node_after_join->type == ActionsDAG::ActionType::INPUT ||
            node_after_join->type == ActionsDAG::ActionType::COLUMN)
            continue;
        if (JoinActionRef(node_after_join, expression_actions).fromLeft())
            result.insert(JoinTableSide::Left);
        if (JoinActionRef(node_after_join, expression_actions).fromRight())
            result.insert(JoinTableSide::Right);
        if (result.size() == 2)
            break;
    }
    return result;
}

JoinStepLogical::~JoinStepLogical() = default;

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

std::string_view joinTypePretty(JoinKind join_kind, JoinStrictness strictness)
{
    /*
     * Inner Join: ⋈ (Unicode U+22C8)
     * Left Outer Join: ⟕ (Unicode U+27D5)
     * Right Outer Join: ⟖ (Unicode U+27D6)
     * Full Outer Join: ⟗ (Unicode U+27D7)
     * Semi Join: ⋉ (Unicode U+22C9)
     * Right Semi Join: ⋊ (Unicode U+22CA)
     * Anti Join: ⋉̸ (Unicode U+22C9 U+0338)
     * Right Anti Join: ⋊̸ (Unicode U+22CA U+0338)
     * Cross Join: × (Unicode U+00D7)
     * Paste Join: ⟘ (Unicode U+27D8)
     */

    constexpr auto def_symb = "?";
    auto symbols = std::array{
        ///         Inner     Left            Right           Full      Cross    Comma      Paste
        std::array{"\u22C8", "\u27D5",       "\u27D6",       "\u27D7", "\u00D7", ",",      "\u27D8"},  /// All/Any
        std::array{"\u22C8", "\u22C9",       "\u22CA",       "\u22C8", def_symb, def_symb, def_symb},  /// Semi
        std::array{def_symb, "\u22C9\u0338", "\u22CA\u0338", def_symb, def_symb, def_symb, def_symb},  /// Anti
    };

    size_t row = 0;
    switch (strictness)
    {
        case JoinStrictness::Semi: row = 1; break;
        case JoinStrictness::Anti: row = 2; break;
        default: break;
    }
    size_t col = 0;
    switch (join_kind)
    {
        case JoinKind::Inner: col = 0; break;
        case JoinKind::Left: col = 1; break;
        case JoinKind::Right: col = 2; break;
        case JoinKind::Full: col = 3; break;
        case JoinKind::Cross: col = 4; break;
        case JoinKind::Comma: col = 5; break;
        case JoinKind::Paste: col = 6; break;
    }
    return symbols[row][col];
}

std::string_view joinTypePretty(const JoinOperator & join_operator)
{
    return joinTypePretty(join_operator.kind, join_operator.strictness);
}

String JoinStepLogical::getReadableRelationName() const
{
    if (left_table_label.empty() || right_table_label.empty())
        return "";
    return fmt::format("{} {} {}", left_table_label, joinTypePretty(join_operator), right_table_label);
}

std::vector<std::pair<String, String>> JoinStepLogical::describeJoinProperties() const
{
    std::vector<std::pair<String, String>> description;

    auto readable_relation_name = getReadableRelationName();
    if (!readable_relation_name.empty())
        description.emplace_back("Join", std::move(readable_relation_name));

    description.emplace_back("ResultRows", result_rows_estimation ? toString(result_rows_estimation.value()) : "unknown");

    description.emplace_back("Type", toString(join_operator.kind));
    description.emplace_back("Strictness", toString(join_operator.strictness));
    description.emplace_back("Locality", toString(join_operator.locality));
    description.emplace_back("Expression", formatJoinCondition(join_operator.expression));

    return description;
}

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);

    for (const auto & [name, value] : describeJoinProperties())
        settings.out << prefix << name << ": " << value << '\n';

    settings.out << prefix << "Expression:\n";
    auto actions_dag = expression_actions.getActionsDAG();
    ExpressionActions(actions_dag->clone()).describeActions(settings.out, prefix);

    settings.out << prefix << "Expression Sources:\n";
    for (const auto * input_ptr : actions_dag->getInputs())
        settings.out << prefix << JoinActionRef(input_ptr, expression_actions).dump() << "\n";
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
    Block header(actions_dag->getResultColumns());

    for (auto & column : header)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    if (!header.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is empty, actions_dag: {}", actions_dag->dumpDAG());

    output_header = std::make_shared<const Block>(std::move(header));
}

JoinStepLogicalLookup::JoinStepLogicalLookup(QueryPlan child_plan_, PreparedJoinStorage prepared_join_storage_, bool use_nulls_)
    : ISourceStep(child_plan_.getCurrentHeader())
    , prepared_join_storage(std::move(prepared_join_storage_))
    , child_plan(std::move(child_plan_))
    , use_nulls(use_nulls_)
{
}

void JoinStepLogicalLookup::initializePipeline(QueryPipelineBuilder & pipeline_builder, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    QueryPlanOptimizationSettings optimization_settings({}, {}, {}, {}, {}, false);
    pipeline_builder = std::move(*child_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings, /* do_optimize */ false));
}

void JoinStepLogicalLookup::optimize(const QueryPlanOptimizationSettings & optimization_settings)
{
    if (optimized)
        return;
    optimized = true;
    child_plan.optimize(optimization_settings);
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
    {
        return JoinActionRef::transform({condition}, [](auto & dag, auto && nodes)
        {
            JoinActionRef::AddFunction function_and(JoinConditionOperator::And);
            DataTypePtr uint8_ty = std::make_shared<DataTypeUInt8>();
            const auto & rhs_node = dag.addColumn(ColumnWithTypeAndName(uint8_ty->createColumnConst(1, 1), uint8_ty, "true"));
            nodes.push_back(&rhs_node);
            return function_and(dag, nodes);
        });
    }
    return condition;
}

bool canPushDownFromOn(const JoinOperator & join_operator, std::optional<JoinTableSide> side = {})
{
    /// Filter pushdown for PASTE JOIN is *disabled* to preserve positional alignment
    bool is_suitable_kind = join_operator.kind == JoinKind::Inner
        || join_operator.kind == JoinKind::Cross
        || join_operator.kind == JoinKind::Comma
        || (side == JoinTableSide::Left && join_operator.kind == JoinKind::Right)
        || (side == JoinTableSide::Right && join_operator.kind == JoinKind::Left);

    return is_suitable_kind && join_operator.strictness == JoinStrictness::All;
}

using NameViewToNodeMapping = std::unordered_map<std::string_view, const ActionsDAG::Node *>;


struct JoinPlanningContext
{
    NameViewToNodeMapping actions_after_join_map;
    bool is_storage_join;
};

void predicateOperandsToCommonType(JoinActionRef & left_node, JoinActionRef & right_node, const JoinSettings & join_settings, const JoinPlanningContext & planning_context)
{
    const auto & left_type = left_node.getType();
    const auto & right_type = right_node.getType();

    if (!join_settings.allow_dynamic_type_in_join_keys)
    {
        bool is_left_key_dynamic = hasDynamicType(left_type);
        bool is_right_key_dynamic = hasDynamicType(right_type);

        if (is_left_key_dynamic || is_right_key_dynamic)
        {
            throw DB::Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "JOIN on keys with Dynamic type is not supported: key {} has type {}. In order to use this key in JOIN you should cast it to any other type",
                is_left_key_dynamic ? left_node.getColumnName() : right_node.getColumnName(),
                is_left_key_dynamic ? left_type->getName() : right_type->getName());
        }
    }

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

    auto cast_transform = [&common_type, &planning_context](auto & dag, auto && nodes)
    {
        auto arg = nodes.at(0);
        auto mapped_it = planning_context.actions_after_join_map.find(arg->result_name);
        if (mapped_it != planning_context.actions_after_join_map.end() && mapped_it->second->result_type->equals(*common_type))
            return mapped_it->second;
        return &dag.addCast(*arg, common_type, {}, nullptr);
    };
    if (!left_type->equals(*common_type))
        left_node = JoinActionRef::transform({left_node}, cast_transform);

    if (planning_context.is_storage_join)
    {
        if (!right_type->equals(*removeNullableOrLowCardinalityNullable(common_type)))
            right_node = JoinActionRef::transform({right_node}, cast_transform);
    }
    else
    {
        if (!right_type->equals(*common_type))
            right_node = JoinActionRef::transform({right_node}, cast_transform);
    }
}

bool addJoinPredicatesToTableJoin(std::vector<JoinActionRef> & predicates, TableJoin::JoinOnClause & table_join_clause,
    std::vector<JoinActionRef> & used_expressions, const JoinSettings & join_settings, const JoinPlanningContext & planning_context)
{
    bool has_join_predicates = false;
    std::vector<JoinActionRef> new_predicates;

    for (auto & pred : predicates)
    {
        auto & predicate = new_predicates.emplace_back(std::move(pred));
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        predicateOperandsToCommonType(lhs, rhs, join_settings, planning_context);
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
        used_expressions.push_back(lhs);
        used_expressions.push_back(rhs);
        new_predicates.pop_back();
    }

    predicates = std::move(new_predicates);

    return has_join_predicates;
}


static SharedHeader blockWithActionsDAGOutput(const ActionsDAG & actions_dag)
{
    ColumnsWithTypeAndName columns;
    columns.reserve(actions_dag.getOutputs().size());
    for (const auto & node : actions_dag.getOutputs())
        columns.emplace_back(node->column ? node->column : node->result_type->createColumn(), node->result_type, node->result_name);
    return std::make_shared<const Block>(Block{columns});
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
    std::vector<JoinActionRef> & used_expressions,
    const JoinSettings & join_settings,
    const JoinPlanningContext & planning_context,
    bool throw_on_error)
{
    if (join_expressions.size() != 1)
        return false;

    auto & join_expression = join_expressions.front();
    if (!join_expression.isFunction(JoinConditionOperator::Or))
        return false;

    size_t initial_clauses_num = table_join_clauses.size();
    std::vector<JoinActionRef> disjunctive_conditions = join_expression.getArguments();
    bool has_residual_condition = false;
    for (const auto & expr : disjunctive_conditions)
    {
        std::vector<JoinActionRef> join_condition = {expr};
        if (expr.isFunction(JoinConditionOperator::And))
            join_condition = expr.getArguments();

        auto & table_join_clause = table_join_clauses.emplace_back();
        bool has_keys = addJoinPredicatesToTableJoin(join_condition, table_join_clause, used_expressions, join_settings, planning_context);
        if (!has_keys)
        {
            table_join_clauses.resize(initial_clauses_num);
            if (!throw_on_error)
                return false;

            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}", formatJoinCondition({expr}));
        }

        if (auto left_pre_filter_condition = concatConditions(join_condition, JoinTableSide::Left))
        {
            table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
            used_expressions.push_back(left_pre_filter_condition);
        }

        if (auto right_pre_filter_condition = concatConditions(join_condition, JoinTableSide::Right))
        {
            table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
            used_expressions.push_back(right_pre_filter_condition);
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
    const TableJoin & table_join,
    size_t max_step_description_length)
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
        sorting_step->setStepDescription(fmt::format("Sort {} before JOIN", join_table_side), max_step_description_length);
        node = &nodes.emplace_back(QueryPlan::Node{std::move(sorting_step), {node}});
    };

    auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
    auto add_create_set = [&](QueryPlan::Node *& node, const Names & key_names, JoinTableSide join_table_side)
    {
        auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
            node->step->getOutputHeader(), key_names, join_settings.max_rows_in_set_to_optimize_join, crosswise_connection, join_table_side);
        creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_table_side), max_step_description_length);

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
    bool has_non_const_keys = has_non_const(*left_node->step->getOutputHeader(), join_clause.key_names_left)
        && has_non_const(*right_node->step->getOutputHeader() , join_clause.key_names_right);
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

    auto * join_left_node = node.children[0];
    makeExpressionNodeOnTopOf(*join_left_node, std::move(left_pre_join_actions), nodes, makeDescription("Left Join Actions"));

    node.step = std::make_unique<FilledJoinStep>(join_left_node->step->getOutputHeader(), join_ptr, join_settings.max_block_size);

    post_join_actions.appendInputsForUnusedColumns(*node.step->getOutputHeader());
    makeFilterNodeOnTopOf(
        node, std::move(post_join_actions),
        residual_filter_condition.first, residual_filter_condition.second,
        nodes, makeDescription("Post Join Actions"));
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

    makeExpressionNodeOnTopOf(*join_left_node, std::move(left_pre_join_actions), nodes, makeDescription("Left Pre Join Actions"));

    makeExpressionNodeOnTopOf(*join_right_node, std::move(right_pre_join_actions), nodes, makeDescription("Right Pre Join Actions"));

    if (const auto * fsmjoin = dynamic_cast<const FullSortingMergeJoin *>(join_ptr.get()))
        addSortingForMergeJoin(fsmjoin, join_left_node, join_right_node, nodes,
            sorting_settings, join_settings, fsmjoin->getTableJoin(), optimization_settings.max_step_description_length);

    auto required_output_from_join = post_join_actions.getRequiredColumnsNames();
    auto join_step = std::make_unique<JoinStep>(
        join_left_node->step->getOutputHeader(),
        join_right_node->step->getOutputHeader(),
        join_ptr,
        join_settings.max_block_size,
        join_settings.min_joined_block_size_rows,
        join_settings.min_joined_block_size_bytes,
        optimization_settings.max_threads,
        NameSet(required_output_from_join.begin(), required_output_from_join.end()),
        false /*optimize_read_in_order*/,
        true /*use_new_analyzer*/);
    join_step->setOptimized();
    node.step = std::move(join_step);

    node.children = {join_left_node, join_right_node};

    post_join_actions.appendInputsForUnusedColumns(*node.step->getOutputHeader());
    makeFilterNodeOnTopOf(
        node, std::move(post_join_actions),
        residual_filter_condition.first, residual_filter_condition.second,
        nodes, makeDescription("Post Join Actions"));
}

static QueryPlanNode buildPhysicalJoinImpl(
    std::vector<QueryPlanNode *> children,
    JoinOperator join_operator,
    JoinExpressionActions expression_actions,
    JoinSettings join_settings,
    JoinAlgorithmParams join_algorithm_params,
    SortingStep::Settings sorting_settings,
    const ActionsDAG::NodeRawConstPtrs & actions_after_join,
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan::Nodes & nodes)
{
    auto * logical_lookup = typeid_cast<JoinStepLogicalLookup *>(children.back()->step.get());

    auto table_join = std::make_shared<TableJoin>(join_settings, logical_lookup && logical_lookup->useNulls(),
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

    bool is_join_without_expression = isCrossOrComma(join_operator.kind) || isPaste(join_operator.kind);
    /// When we do JOIN ON NULL or JOIN ON 1 we create dummy columns and in fact joining on 1 = 0 or 1 = 1.
    /// For INNER JOIN we could just do CROSS, but for OUTER result depends on whether any table is empty or not.
    if ((!is_join_without_expression && join_expression.empty()) ||
        (join_expression.size() == 1
            && join_expression[0].getType()->onlyNull()
            && std::get<0>(join_expression[0].asBinaryPredicate()) == JoinConditionOperator::Unknown))
    {
        UInt8 rhs_value = join_expression.empty() ? 1 : 0;
        join_expression.clear();

        auto actions_dag = expression_actions.getActionsDAG();

        auto dt = std::make_shared<DataTypeUInt8>();

        ColumnWithTypeAndName lhs_column(dt->createColumnConst(1, 1), dt, "__lhs_const");
        JoinActionRef lhs(&actions_dag->addColumn(lhs_column), expression_actions);
        lhs.setSourceRelations(BitSet().set(0));

        ColumnWithTypeAndName rhs_column(dt->createColumnConst(1, rhs_value), dt, "__rhs_const");
        JoinActionRef rhs(&actions_dag->addColumn(rhs_column), expression_actions);
        rhs.setSourceRelations(BitSet().set(1));

        join_expression.push_back(JoinActionRef::transform({lhs, rhs}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }

    std::vector<JoinActionRef> used_expressions;

    JoinPlanningContext planning_context;
    planning_context.is_storage_join = bool(prepared_join_storage);
    for (const auto * node : actions_after_join)
    {
        if (node->type == ActionsDAG::ActionType::ALIAS)
        planning_context.actions_after_join_map[node->result_name] = node->children.at(0);
    }


    bool is_disjunctive_condition = false;
    auto & table_join_clauses = table_join->getClauses();
    if (!is_join_without_expression)
    {
        bool has_keys = addJoinPredicatesToTableJoin(join_expression, table_join_clauses.emplace_back(), used_expressions, join_settings, planning_context);

        if (!has_keys && join_operator.strictness != JoinStrictness::Asof)
        {
            bool can_convert_to_cross = (isInner(join_operator.kind) || isCrossOrComma(join_operator.kind))
                && TableJoin::isEnabledAlgorithm(join_settings.join_algorithms, JoinAlgorithm::HASH)
                && join_operator.strictness == JoinStrictness::All;

            table_join_clauses.pop_back();
            is_disjunctive_condition = tryAddDisjunctiveConditions(
                join_expression, table_join_clauses, used_expressions, join_settings, planning_context, !can_convert_to_cross);

            if (!is_disjunctive_condition)
            {
                if (!can_convert_to_cross)
                    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                        formatJoinCondition(join_expression));

                join_operator.kind = JoinKind::Cross;
                join_operator.residual_filter.append_range(join_expression);
                join_expression.clear();
            }
        }
    }
    else if (!join_expression.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected JOIN ON expression {} for {} JOIN",
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

            predicateOperandsToCommonType(lhs, rhs, join_settings, planning_context);

            used_expressions.push_back(lhs);
            used_expressions.push_back(rhs);

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
        used_expressions.push_back(left_pre_filter_condition);
    }

    if (auto right_pre_filter_condition = concatConditions(join_expression, JoinTableSide::Right))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
        used_expressions.push_back(right_pre_filter_condition);
    }

    join_operator.residual_filter.append_range(join_expression);
    JoinActionRef residual_filter_condition = concatConditions(join_operator.residual_filter);
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> actions_after_join_fold;
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

        actions_after_join_fold[action] = action;
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
                if (actions_after_join_fold.contains(node))
                    continue;
                actions_after_join_fold[node] = node;
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
        used_expressions.emplace_back(action, expression_actions);
    }

    for (const auto * action : required_residual_nodes)
        used_expressions.emplace_back(action, expression_actions);

    for (const auto * child : children)
    {
        /// We expect dag inputs to be a subset of child step header columns.
        /// If column child step returns duplicate columns
        /// we need to find corresponding duplicates in dag inputs, which will be different nodes.
        const auto & dag_inputs = expression_actions.getActionsDAG()->getInputs();
        std::unordered_map<std::string_view, std::deque<const ActionsDAG::Node *>> name_to_nodes;
        for (const auto * node : dag_inputs)
            name_to_nodes[node->result_name].push_back(node);

        for (const auto & column : *child->step->getOutputHeader())
        {
            auto input_it = name_to_nodes.find(column.name);

            if (input_it == name_to_nodes.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot find input column {} on its position in inputs of expression actions DAG, expected inputs {} in {}",
                    column.name,
                    fmt::join(children | std::views::transform([](const auto & c) { return fmt::format("[{}]", c->step->getOutputHeader()->dumpNames()); }), ", "),
                    expression_actions.getActionsDAG()->dumpDAG());


            used_expressions.emplace_back(input_it->second.front(), expression_actions);
            input_it->second.pop_front();
        }
    }

    {
        std::unordered_set<const ActionsDAG::Node *> seen_used_expressions;
        auto it = std::remove_if(used_expressions.begin(), used_expressions.end(),
            [&](const JoinActionRef & x) { return !seen_used_expressions.insert(x.getNode()).second; });
        used_expressions.erase(it, used_expressions.end());
    }

    ActionsDAG left_dag = JoinExpressionActions::getSubDAG(used_expressions | std::views::filter([](const auto & node) { return node.fromLeft() || node.fromNone(); }));
    ActionsDAG right_dag = JoinExpressionActions::getSubDAG(used_expressions | std::views::filter([](const auto & node) { return node.fromRight(); }));

    if (logical_lookup && prepared_join_storage.storage_key_value)
    {
        right_dag.mergeInplace(
            JoinExpressionActions::getSubDAG(
                actions_after_join
                    | std::views::transform([&](const auto * action) { return JoinActionRef(action, expression_actions); })
                    | std::views::filter([](const auto & action) { return action.fromRight(); })));
    }

    auto common_dag = expression_actions.getActionsDAG();

    bool can_remove_residual_filter = false;
    auto & required_output_nodes = common_dag->getOutputs();
    if (residual_filter_condition && !std::ranges::contains(required_output_nodes, residual_filter_condition.getNode()))
    {
        can_remove_residual_filter = true;
        required_output_nodes.emplace_back(residual_filter_condition.getNode());
    }

    ActionsDAG residual_dag = ActionsDAG::foldActionsByProjection(actions_after_join_fold, required_output_nodes);

    table_join->setInputColumns(
        left_dag.getNamesAndTypesList(),
        right_dag.getNamesAndTypesList());
    table_join->setUsedColumns(residual_dag.getRequiredColumnsNames());
    table_join->setJoinOperator(join_operator);

    SharedHeader left_sample_block = blockWithActionsDAGOutput(left_dag);
    SharedHeader right_sample_block = blockWithActionsDAGOutput(right_dag);

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
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan::Nodes & nodes)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || node.children.empty())
    {
        if (node.step)
        {
            const auto & step = *node.step;
            auto type_name = demangle(typeid(step).name());
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected JoinStepLogical, got '{}' of type {} with {} children", node.step->getName(), type_name, node.children.size());
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected JoinStepLogical, got {}", !join_step ? "nullptr" : "empty children");
    }

    UInt64 hash_table_key_hash = optimization_settings.collect_hash_table_stats_during_joins ? join_step->getRightHashTableCacheKey() : 0;

    if (!join_step->join_algorithm_params)
    {
        join_step->join_algorithm_params = std::make_unique<JoinAlgorithmParams>(
            join_step->join_settings,
            optimization_settings.max_threads,
            hash_table_key_hash,
            optimization_settings.max_entries_for_hash_table_stats,
            optimization_settings.initial_query_id,
            optimization_settings.lock_acquire_timeout);

        if (join_step->right_rows_estimation)
            join_step->join_algorithm_params->rhs_size_estimation = join_step->right_rows_estimation;

        if (hash_table_key_hash)
        {
            StatsCollectingParams params{
                /*key_=*/hash_table_key_hash,
                /*enable=*/ optimization_settings.collect_hash_table_stats_during_joins,
                optimization_settings.max_entries_for_hash_table_stats,
                optimization_settings.max_size_to_preallocate_for_joins};
            auto & hash_table_stats = getHashTablesStatistics<HashJoinEntry>();
            if (auto hint = hash_table_stats.getSizeHint(params))
                join_step->join_algorithm_params->rhs_size_estimation = hint->source_rows;
        }
    }

    auto new_node = buildPhysicalJoinImpl(
        node.children,
        join_step->join_operator,
        std::move(join_step->expression_actions),
        join_step->join_settings,
        *join_step->join_algorithm_params,
        join_step->sorting_settings,
        join_step->actions_after_join,
        optimization_settings,
        nodes);

    node = std::move(new_node);
}

std::optional<ActionsDAG::ActionsForFilterPushDown> JoinStepLogical::getFilterActions(JoinTableSide side, const SharedHeader & stream_header)
{
    if (join_operator.strictness != JoinStrictness::All)
        return {};

    auto & join_expression = join_operator.expression;

    if (!canPushDownFromOn(join_operator, side))
        return {};

    if (auto filter_condition = concatConditions(join_expression, side))
        return ActionsDAG::createActionsForConjunction({filter_condition.getNode()}, stream_header->getColumnsWithTypeAndName());

    return {};
}

void remapNodes(ActionsDAG::NodeRawConstPtrs & keys, const ActionsDAG::NodeMapping & node_map)
{
    for (const auto *& key : keys)
    {
        if (auto it = node_map.find(key); it != node_map.end())
            key = it->second;
    }
}

ActionsDAG cloneSubdagWithInputs(const SharedHeader & stream_header, ActionsDAG::NodeRawConstPtrs & keys)
{
    ActionsDAG dag(stream_header->getColumnsWithTypeAndName());

    ActionsDAG::NodeMapping node_map;
    auto second_dag = ActionsDAG::cloneSubDAG(keys, node_map, false);

    remapNodes(keys, node_map);
    node_map.clear();

    dag.mergeInplace(std::move(second_dag), node_map, true);
    remapNodes(keys, node_map);

    dag.getOutputs() = dag.getInputs();
    dag.getOutputs().append_range(keys | std::views::filter([&](const auto * node) { return node->type != ActionsDAG::ActionType::INPUT; }));

    return dag;
}

std::optional<std::pair<JoinStepLogical::ActionsDAGWithKeys, JoinStepLogical::ActionsDAGWithKeys>>
JoinStepLogical::preCalculateKeys(const SharedHeader & left_header, const SharedHeader & right_header)
{
    auto & join_expression = join_operator.expression;

    ActionsDAG::NodeRawConstPtrs left_keys;
    ActionsDAG::NodeRawConstPtrs right_keys;

    for (auto & expr : join_expression)
    {
        auto [predicate_op, lhs, rhs] = expr.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals)
            continue;

        const auto * left_node = lhs.getNode();
        const auto * right_node = rhs.getNode();
        if (lhs.fromLeft() && rhs.fromRight())
        {
            left_keys.push_back(left_node);
            right_keys.push_back(right_node);
        }
        else if (lhs.fromRight() && rhs.fromLeft())
        {
            left_keys.push_back(right_node);
            right_keys.push_back(left_node);
        }
        else
        {
            continue;
        }

        /// Replace keys expression with calculated inputs
        /// We also could possibly remove some nodes from dag,
        /// but they will simply remain unused when converting to a physical step
        if (left_node->type != ActionsDAG::ActionType::INPUT ||
            right_node->type != ActionsDAG::ActionType::INPUT)
        {
            if (left_node->type != ActionsDAG::ActionType::INPUT)
                lhs = expression_actions.addInput(left_node->result_name, left_node->result_type, lhs.fromLeft() ? 0 : 1);
            if (right_node->type != ActionsDAG::ActionType::INPUT)
                rhs = expression_actions.addInput(right_node->result_name, right_node->result_type, rhs.fromRight() ? 1 : 0);
            expr = JoinActionRef::transform({lhs, rhs}, JoinActionRef::AddFunction(predicate_op));
        }
    }

    if (left_keys.empty() || right_keys.empty())
        return {};

    auto left_dag = cloneSubdagWithInputs(left_header, left_keys);
    updateInputHeader(std::make_shared<Block>(left_dag.getResultColumns()), 0);

    auto right_dag = cloneSubdagWithInputs(right_header, right_keys);
    updateInputHeader(std::make_shared<Block>(right_dag.getResultColumns()), 1);

    return std::make_optional(std::pair{
        ActionsDAGWithKeys{std::move(left_dag), std::move(left_keys)},
        ActionsDAGWithKeys{std::move(right_dag), std::move(right_keys)},
    });
}


void JoinStepLogical::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    join_settings.updatePlanSettings(settings);
    sorting_settings.updatePlanSettings(settings);
}

static void serializeNodeList(
    WriteBuffer & out,
    const std::unordered_map<const ActionsDAG::Node *, size_t> & node_to_id,
    const ActionsDAG::NodeRawConstPtrs & nodes)
{
    writeVarUInt(nodes.size(), out);
    for (const auto * node : nodes)
    {
        if (auto it = node_to_id.find(node); it != node_to_id.end())
            writeVarUInt(it->second, out);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find node '{}' in node map", node->result_name);
    }
}

void JoinStepLogical::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    writeIntBinary(flags, ctx.out);

    writeVarUInt(1, ctx.out);
    auto actions_dag = expression_actions.getActionsDAG();
    actions_dag->serialize(ctx.out, ctx.registry);

    join_operator.serialize(ctx.out, actions_dag.get());
    serializeNodeList(ctx.out, actions_dag->getNodeToIdMap(), actions_after_join);
}

static ActionsDAG::NodeRawConstPtrs deserializeNodeList(ReadBuffer & in, const ActionsDAG::NodeRawConstPtrs & id_to_node)
{
    size_t num_nodes;
    readVarUInt(num_nodes, in);

    size_t max_node_id = id_to_node.size();

    ActionsDAG::NodeRawConstPtrs nodes(num_nodes);
    for (size_t i = 0; i < num_nodes; ++i)
    {
        size_t node_id;
        readVarUInt(node_id, in);
        if (node_id >= max_node_id)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Node id {} is out of range, must be less than {}", node_id, max_node_id);

        nodes[i] = id_to_node[node_id];
    }
    return nodes;
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
    auto id_to_node = actions_dag.getIdToNode();

    auto left_header = ctx.input_headers.front();
    auto right_header = ctx.input_headers.back();
    JoinExpressionActions expression_actions(*left_header, *right_header, std::move(actions_dag));

    auto join_operator = JoinOperator::deserialize(ctx.in, expression_actions);
    auto actions_after_join = deserializeNodeList(ctx.in, id_to_node);

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
    ActionsDAG::NodeMapping node_map;
    auto new_expression_actions = expression_actions.clone(node_map);

    auto remap = [&](const auto * node)
    {
        auto it = node_map.find(node);
        if (it == node_map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find node in node map");
        return it->second;
    };

    auto new_actions_dag = new_expression_actions.getActionsDAG();
    auto new_actions_after_join = actions_after_join;
    for (const auto * & action : new_actions_after_join)
        action = remap(action);
    for (auto & action : new_join_operator.expression)
        action = JoinActionRef(remap(action.getNode()), new_expression_actions);
    for (auto & action : new_join_operator.residual_filter)
        action = JoinActionRef(remap(action.getNode()), new_expression_actions);

    auto result_step = std::make_unique<JoinStepLogical>(
        getInputHeaders().front(), getInputHeaders().back(),
        std::move(new_join_operator),
        std::move(new_expression_actions),
        std::move(new_actions_after_join),
        join_settings,
        sorting_settings);
    result_step->setStepDescription(*this);
    return result_step;
}

void JoinStepLogical::addConditions(ActionsDAG actions_dag)
{
    ActionsDAG::NodeRawConstPtrs conditions;
    expression_actions.getActionsDAG()->mergeNodes(std::move(actions_dag), &conditions);
    for (const auto * node : conditions)
        join_operator.expression.emplace_back(node, expression_actions);
}

void registerJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Join", JoinStepLogical::deserialize);
}


}
