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

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Core/Block.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TypeTree.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinUtils.h>
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

namespace
{

/// The columns a subquery projects. Empty when the node is not a subquery, and when it is correlated,
/// whose columns are only known once it is decorrelated.
[[nodiscard]] NamesAndTypes getSubqueryProjectionColumns(const QueryTreeNodePtr & subquery)
{
    if (const auto * node = subquery->as<QueryNode>())
        return node->isCorrelated() ? NamesAndTypes{} : node->getProjectionColumns();

    if (const auto * node = subquery->as<UnionNode>())
        return node->isCorrelated() ? NamesAndTypes{} : node->computeProjectionColumns();

    return {};
}

/// The join is an implementation detail of `IN`, so the join size limits of the query do not apply to it.
void makeJoinUnbounded(JoinStepLogical & join_step)
{
    auto & join_settings = join_step.getJoinSettings();
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    join_settings.join_overflow_mode = OverflowMode::THROW;
}

/// A constant the join carries over to the matched outer rows, the unmatched ones get 0 from the outer join.
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
[[nodiscard]] const ActionsDAG::Node & addNodesForNullKeyMask(
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
/// `JoinLogical` step on the path of a correlated column, which a DAG reads as a `PLACEHOLDER`.
[[nodiscard]] bool readsCorrelatedColumn(const ActionsDAG::Node & node)
{
    if (node.type == ActionsDAG::ActionType::PLACEHOLDER)
        return true;

    return std::ranges::any_of(node.children, [](const ActionsDAG::Node * child) { return readsCorrelatedColumn(*child); });
}

struct JoinKeyActions
{
    ActionsDAG actions;
    /// One per column of the subquery.
    Names key_column_names;
};

/// Removes the computation of the `IN` from the actions and keeps only its key.
[[nodiscard]] JoinKeyActions buildJoinKeyActions(ActionsDAG actions, const ActionsDAG::Node & in_node, const QueryTreeNodePtr & subquery, const Block & input_header, const ContextPtr & query_context)
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

        /// The cast a set applies to the key before it probes, which it builds without the settings of the query. The query
        /// may cast the key itself with its own settings, so this cast gets a name of its own.
        auto key_type = removeNullable(removeLowCardinality(key_output->result_type));
        auto target_type = removeNullable(removeLowCardinality(subquery_columns[i].type));
        if (key_type->getName() != target_type->getName())
            key_output = &actions.addAccurateCastOrNull(
                *key_output, target_type, fmt::format("__key_{}_of_{}", i, in_node.result_name), /*context=*/ nullptr);

        key_column_names.push_back(add_output(*key_output));
    }

    /// The `NULL` mask reads the key after the join.
    if (isNullableOrLowCardinalityNullable(in_node.result_type))
        add_output(key);

    actions.removeUnusedActions(/*allow_remove_inputs=*/ false);
    return {std::move(actions), std::move(key_column_names)};
}

}

bool canRewriteInToJoin(const ActionsDAG::Node & in_node, const QueryTreeNodePtr & subquery)
{
    /// Also excludes `nullIn` and `globalIn`.
    const auto function_name = in_node.function_base->getName();
    if (function_name != "in" && function_name != "notIn")
        return false;

    if (in_node.children.size() != 2)
        return false;

    auto subquery_columns = getSubqueryProjectionColumns(subquery);
    if (subquery_columns.empty())
        return false;

    const auto & left_key = *in_node.children[0];
    if (left_key.type == ActionsDAG::ActionType::COLUMN)
        return false;

    if (isNothingOrNullableNothing(in_node.result_type))
        return false;

    /// A tuple written as `(a, b)` is compared element by element.
    DataTypes key_types;
    if (subquery_columns.size() > 1 && left_key.type == ActionsDAG::ActionType::FUNCTION && left_key.function_base->getName() == "tuple"
        && left_key.children.size() == subquery_columns.size())
        key_types = left_key.children | std::views::transform([](const ActionsDAG::Node * child) { return child->result_type; }) | std::ranges::to<DataTypes>();
    else if (subquery_columns.size() == 1)
        key_types = {left_key.result_type};
    else if (const auto * key_tuple = typeid_cast<const DataTypeTuple *>(left_key.result_type.get()))
        key_types = key_tuple->getElements();

    if (key_types.size() != subquery_columns.size())
        return false;

    for (size_t i = 0; i < key_types.size(); ++i)
    {
        auto column_base = removeNullable(removeLowCardinality(subquery_columns[i].type));
        auto key_base = removeNullable(removeLowCardinality(key_types[i]));
        if (key_base->hasDynamicStructure() || isVariant(key_base) || column_base->hasDynamicStructure() || isVariant(column_base))
            return false;

        /// The join compares the key with `equals`, which a set does not need. A `Variant` counts as comparable
        /// even when one of its alternatives is not, and a sort-merge join then treats all such values as equal.
        /// A set does not match a timestamp that the cast to the column type changes, e.g. by dropping a sub-second
        /// part, while the join would match the changed value.
        /// A cast to a tuple, or to a type that cannot be inside `Nullable`, throws on a value the column type cannot hold.
        const auto not_comparable = [](const IDataType & type) { return !type.isComparableForEquality(); };
        if (anyInTypeTree(*key_base, not_comparable) || anyInTypeTree(*column_base, not_comparable)
            || (isDateTime64(key_base) && key_base->getName() != column_base->getName())
            || (key_base->getName() != column_base->getName() && (!column_base->canBeInsideNullable() || isTuple(column_base))))
            return false;
    }

    if (readsCorrelatedColumn(left_key))
        return false;

    return true;
}

std::optional<UncorrelatedInSubquery> findInnermostUncorrelatedInSubquery(const ActionsDAG & actions, const NameSet & skipped)
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
            if (node->type != ActionsDAG::ActionType::FUNCTION || node->children.size() != 2 || !node->children[1]->column
                || skipped.contains(node->result_name))
                continue;

            const auto * column_set = typeid_cast<const ColumnSet *>(&node->children[1]->column->getDataColumn());
            const auto * set = column_set ? typeid_cast<const FutureSetFromSubquery *>(column_set->getData().get()) : nullptr;
            if (set && set->getQueryTree())
                return UncorrelatedInSubquery{.in_node = node, .subquery = set->getQueryTree(), .actions = {}};
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
    QueryPlan subquery_plan,
    UsefulSets & useful_sets)
{
    const auto & query_context = planner_context->getQueryContext();

    const auto & in_node = *in_subquery.in_node;
    /// The join delivers the result under the name of the `in` node, so a later step that reads this `IN` by name finds it.
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

    VectorWithMemoryTracking<JoinActionRef> predicates;
    for (size_t i = 0; i < key_column_names.size(); ++i)
    {
        VectorWithMemoryTracking<JoinActionRef> eq_arguments;
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
        UnorderedMapWithMemoryTracking<String, const ActionsDAG::Node *>{},
        /*join_use_nulls=*/false,
        JoinSettings(settings, planner_context->getQueryContext()->getJoinAnalyzeMode()),
        SortingStep::Settings(settings));
    join_step->setStepDescription("JOIN to evaluate IN");
    makeJoinUnbounded(*join_step);

    auto & join_settings = join_step->getJoinSettings();
    auto & algorithms = join_settings.join_algorithms;

    /// `grace_hash` runs the join when no algorithm listed before it can. Without a spill threshold it cannot spill here,
    /// because the size limits that trigger spilling in legacy mode do not apply to this join.
    if (auto grace = std::ranges::find(algorithms, JoinAlgorithm::GRACE_HASH);
        grace != algorithms.end() && join_settings.getEffectiveMaxBytesBeforeExternalJoin() == 0)
    {
        JoinSettings before_grace = join_settings;
        before_grace.join_algorithms.assign(algorithms.begin(), grace);
        if (!JoinCommon::canBeExecutedByEnabledAlgorithm(before_grace, JoinKind::Left, JoinStrictness::Any))
            algorithms.erase(grace);
    }

    if (!JoinCommon::canBeExecutedByEnabledAlgorithm(join_settings, JoinKind::Left, JoinStrictness::Any))
        algorithms = {JoinAlgorithm::HASH};

    QueryPlanPtrs plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(subquery_plan)));

    QueryPlan result_plan;
    result_plan.unitePlans(std::move(join_step), std::move(plans));

    if (is_negated || !in_result_type->equals(DataTypeUInt8{}))
        addStepForMarkerResult(result_plan, in_result_name, in_result_type, is_negated, in_left_argument_name, planner_context);

    query_plan = std::move(result_plan);
}

}
