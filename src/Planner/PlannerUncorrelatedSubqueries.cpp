#include <Core/NamesAndTypes.h>
#include <Planner/PlannerUncorrelatedSubqueries.h>

#include <algorithm>
#include <ranges>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnSet.h>

#include <Core/Block.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace Setting
{

extern const SettingsJoinAlgorithm join_algorithm;
extern const SettingsBool join_use_nulls;
extern const SettingsBool rewrite_in_to_join;
extern const SettingsBool legacy_join_size_limits_trigger_spilling;
extern const SettingsUInt64 max_bytes_before_external_join;
extern const SettingsDouble max_bytes_ratio_before_external_join;

}

namespace
{

/// The join is an implementation detail of `IN`, so the join size limits of the query do not apply to it.
void makeJoinUnbounded(JoinStepLogical & join_step)
{
    auto & join_settings = join_step.getJoinSettings();
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    join_settings.join_overflow_mode = OverflowMode::THROW;
}

/// A constant the join carries over to the matched outer rows; the unmatched ones get 0 from the outer join.
void addStepForMarker(QueryPlan & subquery_plan, const String & marker_name)
{
    const auto & header = subquery_plan.getCurrentHeader();
    ActionsDAG marker_dag(header->getColumnsWithTypeAndName());

    auto result_type = std::make_shared<DataTypeUInt8>();
    const auto & marker = marker_dag.materializeNode(
        marker_dag.addColumn(result_type->createColumnConst(0, 1), result_type, marker_name));
    marker_dag.getOutputs().push_back(&marker);

    auto marker_step = std::make_unique<ExpressionStep>(header, std::move(marker_dag));
    marker_step->setStepDescription("Create the result of IN for the matched rows");
    subquery_plan.addStep(std::move(marker_step));
}

/// `NULL` is not a value the join can match, so a row with a `NULL` key comes out unmatched, with a `false` marker.
/// This would let `NOT IN` keep the row whereas three valued logic in `IN` would drop. This is fixed by wrapping
/// the marker in `if(key is null, NULL, marker)`. This is only required for a single-column `IN`.
const ActionsDAG::Node & addNodesForNullKeyMask(
    ActionsDAG & result_dag,
    const ActionsDAG::Node & marker,
    const String & key_column_name,
    const ContextPtr & query_context)
{
    auto & function_factory = FunctionFactory::instance();

    const auto * key = result_dag.tryFindInOutputs(key_column_name);
    if (!key)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The join that evaluates `IN` did not deliver its key '{}'", key_column_name);
    const auto & key_is_null = result_dag.addFunction(function_factory.get("isNull", query_context), {key}, {});

    auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    auto null_field = Field();
    const auto & null_node = result_dag.addColumn(
        null_type->createColumnConst(0, null_field), null_type, calculateConstantActionNodeName(null_field, null_type));

    return result_dag.addFunction(function_factory.get("if", query_context), {&key_is_null, &null_node, &marker}, {});
}

/// Turns the marker the join produced into the result the expression above expects: masked for a `NULL` key,
/// negated for `NOT IN`, and cast to the type the `IN` function it replaces has.
void addStepForMarkerResult(
    QueryPlan & query_plan,
    const String & result_name,
    const DataTypePtr & result_type,
    bool is_negated,
    const String & key_column_name,
    const PlannerContextPtr & planner_context)
{
    auto & function_factory = FunctionFactory::instance();
    const auto & query_context = planner_context->getQueryContext();

    const auto & header = query_plan.getCurrentHeader();
    ActionsDAG result_dag(header->getColumnsWithTypeAndName());

    const auto * marker = result_dag.tryFindInOutputs(result_name);
    if (!marker)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The join that evaluates `IN` did not deliver its result '{}'", result_name);

    const auto * result = marker;
    if (isNullableOrLowCardinalityNullable(result_type))
        result = &addNodesForNullKeyMask(result_dag, *result, key_column_name, query_context);

    if (is_negated)
        result = &result_dag.addFunction(function_factory.get("not", query_context), {result}, {});

    /// The expression reads the marker as an input of the `IN` function's result type, so the plan has to deliver exactly that type.
    if (!result->result_type->equals(*result_type))
        result = &result_dag.addCast(*result, result_type, {}, query_context);

    const auto & result_alias = result_dag.addAlias(*result, result_name);

    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.reserve(result_dag.getOutputs().size());
    for (const auto * output : result_dag.getOutputs())
        outputs.push_back(output == marker ? &result_alias : output);
    result_dag.getOutputs() = std::move(outputs);

    auto result_step = std::make_unique<ExpressionStep>(header, std::move(result_dag));
    result_step->setStepDescription("Compute the result of IN from the join");
    query_plan.addStep(std::move(result_step));
}

/// Decorrelation rewrites the plan of the subquery the expression belongs to, and it does not support a
/// `JoinLogical` step on the path of a correlated column.
bool readsCorrelatedColumn(const QueryTreeNodePtr & node, const ColumnNodePtrWithHashSet & correlated_columns)
{
    if (auto column_node = std::dynamic_pointer_cast<ColumnNode>(node))
        return correlated_columns.contains(column_node);

    for (const auto & child : node->getChildren())
        if (child && readsCorrelatedColumn(child, correlated_columns))
            return true;

    return false;
}

/// The expressions the join keys on: the left argument of the `IN`, or its elements.
QueryTreeNodes getInToJoinKeyElements(const FunctionNode & function_node)
{
    const auto & left_key = function_node.getArguments().getNodes()[0];

    if (getSubqueryProjectionColumns(function_node.getArguments().getNodes()[1]).size() > 1)
    {
        /// A tuple written as `(a, b)` is compared element by element.
        const auto * tuple_node = left_key->as<FunctionNode>();
        if (tuple_node && tuple_node->getFunctionName() == "tuple")
            return tuple_node->getArguments().getNodes();
    }

    return {left_key};
}

struct JoinKeyActions
{
    ActionsDAG actions;
    /// One per column of the subquery.
    Names key_column_names;
};

/// Removes the computation of the `IN` from `actions` and keeps only its key.
JoinKeyActions buildJoinKeyActions(ActionsDAG actions, const ActionsDAG::Node & in_node, const QueryTreeNodePtr & subquery, const Block & input_header, const ContextPtr & query_context)
{
    const auto subquery_columns = getSubqueryProjectionColumns(subquery);

    /// A tuple written as `(a, b)` is compared element by element.
    const auto & key = *in_node.children.at(0);
    ActionsDAG::NodeRawConstPtrs key_outputs = {&key};
    if (subquery_columns.size() > 1 && key.type == ActionsDAG::ActionType::FUNCTION && key.function_base->getName() == "tuple"
        && key.children.size() == subquery_columns.size())
        key_outputs = key.children;

    /// Split a tuple key into one element per subquery column.
    if (key_outputs.size() != subquery_columns.size())
    {
        chassert(isTuple(key.result_type), "A key that `IN` does not compare column by column has to be a tuple");

        key_outputs.clear();
        auto index_type = std::make_shared<DataTypeUInt64>();
        auto tuple_element = FunctionFactory::instance().get("tupleElement", query_context);
        for (size_t i = 0; i < subquery_columns.size(); ++i)
        {
            const auto & index_node = actions.addColumn(index_type->createColumnConst(0, i + 1), index_type, calculateConstantActionNodeName(i + 1, index_type));
            key_outputs.push_back(&actions.addFunction(tuple_element, {&key, &index_node}, {}));
        }
    }

    auto & outputs = actions.getOutputs();
    std::erase(outputs, &in_node);
    auto column_names = actions.updateHeader(input_header).getNameSet();

    auto add_output = [&](const ActionsDAG::Node & node)
    {
        if (column_names.insert(node.result_name).second)
            outputs.push_back(&node);
        return node.result_name;
    };

    Names key_column_names;
    for (size_t i = 0; i < key_outputs.size(); ++i)
    {
        const auto * key_output = key_outputs[i];

        /// The cast a set applies to the key before it probes.
        auto key_type = removeNullable(removeLowCardinality(key_output->result_type));
        auto target_type = removeNullable(removeLowCardinality(subquery_columns[i].type));
        if (key_type->getName() != target_type->getName())
            key_output = &actions.addAccurateCastOrNull(*key_output, target_type, {}, query_context);

        key_column_names.push_back(add_output(*key_output));
    }

    /// The `NULL` mask reads the key after the join.
    if (isNullableOrLowCardinalityNullable(in_node.result_type))
        add_output(key);

    actions.removeUnusedActions(/*allow_remove_inputs=*/ false);
    return {std::move(actions), std::move(key_column_names)};
}

}

bool canRewriteInToJoin(const QueryTreeNodePtr & in_node, const QueryTreeNodePtr & query_node, const PlannerContext & planner_context)
{
    const auto & function_node = in_node->as<const FunctionNode &>();

    /// Also excludes `nullIn` and `globalIn`.
    if (function_node.getFunctionName() != "in" && function_node.getFunctionName() != "notIn")
        return false;

    const auto & settings = planner_context.getQueryContext()->getSettingsRef();
    if (!settings[Setting::rewrite_in_to_join])
        return false;

    /// `grace_hash` is passed over without a spill threshold.
    const bool grace_hash_can_spill = settings[Setting::legacy_join_size_limits_trigger_spilling]
        || JoinSettings::getMaxBytesBeforeExternalJoin(settings[Setting::max_bytes_before_external_join], settings[Setting::max_bytes_ratio_before_external_join]) > 0;

    const std::vector<JoinAlgorithm> join_algorithms = settings[Setting::join_algorithm];
    if (!std::ranges::any_of(join_algorithms, [&](JoinAlgorithm algorithm)
        {
            return algorithm == JoinAlgorithm::DEFAULT || algorithm == JoinAlgorithm::HASH || algorithm == JoinAlgorithm::PARALLEL_HASH
                || (algorithm == JoinAlgorithm::GRACE_HASH && grace_hash_can_spill) || algorithm == JoinAlgorithm::AUTO
                || algorithm == JoinAlgorithm::PARTIAL_MERGE || algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE;
        }))
        return false;

    const auto & arguments = function_node.getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    auto subquery_columns = getSubqueryProjectionColumns(arguments[1]);
    if (subquery_columns.empty())
        return false;

    /// A set ignores the `WITH TOTALS` row of its subquery, but a join would add it to the query.
    /// A view or a remote table may produce such a row as well.
    if (queryTreeHasWithTotalsInAnySubqueryInJoinTree(arguments[1].get()) || readsViewOrRemoteStorage(arguments[1]))
        return false;

    const auto & left_key = arguments[0];
    if (left_key->as<ConstantNode>())
        return false;

    if (isNothingOrNullableNothing(function_node.getResultType()))
        return false;

    auto key_elements = getInToJoinKeyElements(function_node);
    DataTypes key_types;
    if (key_elements.size() == subquery_columns.size())
        key_types = key_elements | std::views::transform(&IQueryTreeNode::getResultType) | std::ranges::to<DataTypes>();
    else if (const auto * key_tuple = typeid_cast<const DataTypeTuple *>(key_elements.front()->getResultType().get()))
        key_types = key_tuple->getElements();

    if (key_types.size() != subquery_columns.size())
        return false;

    for (size_t i = 0; i < key_types.size(); ++i)
    {
        auto column_base = removeNullable(removeLowCardinality(subquery_columns[i].type));
        auto key_base = removeNullable(removeLowCardinality(key_types[i]));
        if (key_base->hasDynamicStructure() || isVariant(key_base) || column_base->hasDynamicStructure() || isVariant(column_base))
            return false;

        /// The join compares the key with `equals`, which a set does not need.
        if (!key_base->isComparableForEquality() || !column_base->isComparableForEquality())
            return false;

        /// A set turns a key carrying a sub-second part into no match when it casts it to a type that cannot hold one.
        if (isDateTime64(key_base) && !isDateTime64(column_base))
            return false;

        /// A cast to a tuple, or to a type that cannot be inside `Nullable`, throws on a value the column type cannot hold.
        if (key_base->getName() != column_base->getName() && (!column_base->canBeInsideNullable() || isTuple(column_base)))
            return false;
    }

    if (readsCorrelatedColumn(left_key, query_node->as<const QueryNode &>().getCorrelatedColumnsSet()))
        return false;

    return true;
}

std::optional<UncorrelatedInSubquery> findInnermostUncorrelatedInSubquery(const ActionsDAG & actions, const PlannerContext & planner_context)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<std::pair<const ActionsDAG::Node *, bool>> stack;
    for (const auto * output : actions.getOutputs())
        stack.emplace_back(output, false);

    while (!stack.empty())
    {
        auto [node, children_visited] = stack.back();
        stack.pop_back();

        if (children_visited)
        {
            if (node->type != ActionsDAG::ActionType::FUNCTION || node->children.size() != 2 || !node->children[1]->column)
                continue;

            const auto * column_set = typeid_cast<const ColumnSet *>(&node->children[1]->column->getDataColumn());
            if (column_set && planner_context.isSetForJoinRewrite(column_set->getData()))
                return UncorrelatedInSubquery{.in_node = node, .subquery = typeid_cast<const FutureSetFromSubquery &>(*column_set->getData()).getQueryTree(), .actions = {}};
            continue;
        }

        if (!visited.insert(node).second)
            continue;

        stack.emplace_back(node, true);
        for (const auto * child : node->children)
            stack.emplace_back(child, false);
    }

    return {};
}

void buildQueryPlanForUncorrelatedInSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    UncorrelatedInSubquery in_subquery,
    const SelectQueryOptions & select_query_options,
    GlobalPlannerContextPtr subquery_global_planner_context,
    UsefulSets & useful_sets)
{
    const auto & query_context = planner_context->getQueryContext();

    const auto & in_node = *in_subquery.in_node;
    const auto in_result_name = in_node.result_name;
    const auto in_result_type = in_node.result_type;
    const bool is_negated = in_node.function_base->getName() == "notIn";
    const auto in_left_argument_name = in_node.children.at(0)->result_name;

    auto [key_actions, key_column_names] = buildJoinKeyActions(std::move(in_subquery.actions), in_node, in_subquery.subquery, *query_plan.getCurrentHeader(), query_context);
    if (!isPassthroughActions(key_actions))
    {
        auto key_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(key_actions));
        appendSetsFromActionsDAG(key_step->getExpression(), useful_sets);
        key_step->setStepDescription("Compute the left arguments of IN");
        query_plan.addStep(std::move(key_step));
    }

    auto subquery_options = select_query_options.subquery();
    subquery_options.forceMaterializeCTE();
    subquery_options.ignore_limits = false;
    Planner subquery_planner(in_subquery.subquery, subquery_options, std::move(subquery_global_planner_context));
    subquery_planner.buildQueryPlanIfNeeded();
    auto subquery_plan = std::move(subquery_planner).extractQueryPlan();

    ///`IN` matches the columns of its subquery with the elements of its key by position, while a join reads
    /// its input columns by name, so subquery columns must be renamed to have unique names. A column of the
    /// subquery must not carry the name of one of the outer query either.
    auto unique_names_header = *subquery_plan.getCurrentHeader();
    auto outer_column_names = query_plan.getCurrentHeader()->getNames();
    makeUniqueColumnNamesInBlock(unique_names_header, NameSet(outer_column_names.begin(), outer_column_names.end()));
    if (!blocksHaveEqualStructure(unique_names_header, *subquery_plan.getCurrentHeader()))
    {
        auto unique_names_dag = ActionsDAG::makeConvertingActions(
            subquery_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
            unique_names_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            planner_context->getQueryContext());
        auto unique_names_step = std::make_unique<ExpressionStep>(subquery_plan.getCurrentHeader(), std::move(unique_names_dag));
        unique_names_step->setStepDescription("Give the columns of the IN subquery names of their own");
        subquery_plan.addStep(std::move(unique_names_step));
    }

    auto subquery_column_names = subquery_plan.getCurrentHeader()->getNames();
    if (key_column_names.size() != subquery_column_names.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`IN` has {} key columns against {} columns of its subquery", key_column_names.size(), subquery_column_names.size());

    addStepForMarker(subquery_plan, in_result_name);

    auto lhs_header = query_plan.getCurrentHeader();
    auto rhs_header = subquery_plan.getCurrentHeader();

    JoinExpressionActions join_expression_actions(lhs_header->getColumnsWithTypeAndName(), rhs_header->getColumnsWithTypeAndName());

    std::vector<JoinActionRef> predicates;
    for (size_t i = 0; i < key_column_names.size(); ++i)
    {
        std::vector<JoinActionRef> eq_arguments;
        eq_arguments.push_back(join_expression_actions.findNode(key_column_names[i], /*is_input=*/ true));
        eq_arguments.push_back(join_expression_actions.findNode(subquery_column_names[i], /*is_input=*/ true));
        predicates.push_back(JoinActionRef::transform(eq_arguments, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }

    /// The outer rows are the result, plus the marker the subquery side contributes.
    auto lhs_column_names = lhs_header->getNames();
    NameSet output_columns(lhs_column_names.begin(), lhs_column_names.end());
    output_columns.insert(in_result_name);

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();
    auto join_step = std::make_unique<JoinStepLogical>(
        lhs_header,
        rhs_header,
        JoinOperator(JoinKind::Left, JoinStrictness::Any, JoinLocality::Unspecified, std::move(predicates)),
        std::move(join_expression_actions),
        output_columns,
        std::unordered_map<String, const ActionsDAG::Node *>{},
        /*join_use_nulls=*/false,
        JoinSettings(settings, planner_context->getQueryContext()->getJoinAnalyzeMode()),
        SortingStep::Settings(settings));
    join_step->setStepDescription("JOIN to evaluate IN");
    makeJoinUnbounded(*join_step);

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(subquery_plan)));

    QueryPlan result_plan;
    result_plan.unitePlans(std::move(join_step), std::move(plans));

    if (is_negated || !in_result_type->equals(DataTypeUInt8{}))
        addStepForMarkerResult(result_plan, in_result_name, in_result_type, is_negated, in_left_argument_name, planner_context);

    query_plan = std::move(result_plan);
}

}
