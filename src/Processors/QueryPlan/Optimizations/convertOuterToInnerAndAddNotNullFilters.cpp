#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Core/Joins.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB::QueryPlanOptimizations
{

namespace
{

using NodeSet = std::unordered_set<const ActionsDAG::Node *>;
using NullRejectedCache = std::unordered_map<const ActionsDAG::Node *, NodeSet>;

NodeSet collectNullPropagatingInputs(const ActionsDAG::Node * root)
{
    NodeSet result;
    NodeSet visited;
    std::vector<const ActionsDAG::Node *> stack{root};

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (!visited.insert(node).second)
            continue;

        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
            case ActionsDAG::ActionType::PLACEHOLDER:
                /// A column that is not `Nullable` here holds no NULL, so it rejects nothing.
                if (isNullableOrLowCardinalityNullable(node->result_type))
                    result.insert(node);
                break;
            case ActionsDAG::ActionType::ALIAS:
                if (!node->children.empty())
                    stack.push_back(node->children.front());
                break;
            case ActionsDAG::ActionType::FUNCTION:
                if (node->function && node->function->isNullPropagating(node->result_type))
                    stack.insert(stack.end(), node->children.begin(), node->children.end());
                break;
            case ActionsDAG::ActionType::COLUMN:
            case ActionsDAG::ActionType::ARRAY_JOIN:
                break;
        }
    }

    return result;
}

const NodeSet & collectNullRejectedInputs(const ActionsDAG::Node * predicate, NullRejectedCache & cache);

/// Inputs that cannot be NULL in a row that passes `predicate`, because a NULL there would make the predicate NULL.
NodeSet doCollectNullRejectedInputs(const ActionsDAG::Node * predicate, NullRejectedCache & cache)
{
    if (predicate->type == ActionsDAG::ActionType::ALIAS && !predicate->children.empty())
        return collectNullRejectedInputs(predicate->children.front(), cache);

    if (predicate->type == ActionsDAG::ActionType::FUNCTION && predicate->function_base)
    {
        const auto & name = predicate->function_base->getName();

        if (name == "and")
        {
            NodeSet result;
            for (const auto * child : predicate->children)
            {
                const auto & child_result = collectNullRejectedInputs(child, cache);
                result.insert(child_result.begin(), child_result.end());
            }
            return result;
        }

        if (name == "or" && !predicate->children.empty())
        {
            NodeSet result = collectNullRejectedInputs(predicate->children.front(), cache);
            for (size_t i = 1; i < predicate->children.size() && !result.empty(); ++i)
            {
                const auto & other = collectNullRejectedInputs(predicate->children[i], cache);
                std::erase_if(result, [&](const auto * input) { return !other.contains(input); });
            }
            return result;
        }

        if (name == "isNotNull" && predicate->children.size() == 1)
            return collectNullPropagatingInputs(predicate->children.front());
    }

    /// The predicate is rejecting wherever it becomes NULL.
    return collectNullPropagatingInputs(predicate);
}

const NodeSet & collectNullRejectedInputs(const ActionsDAG::Node * predicate, NullRejectedCache & cache)
{
    if (auto it = cache.find(predicate); it != cache.end())
        return it->second;

    return cache.emplace(predicate, doCollectNullRejectedInputs(predicate, cache)).first->second;
}


NodeSet collectNullRejectedInputs(const ActionsDAG::Node * predicate)
{
    NullRejectedCache cache;
    return collectNullRejectedInputs(predicate, cache);
}


/// Names produced by more than one output cannot be mapped to a specific propagated column.
NameSet duplicateOutputNames(const ActionsDAG::NodeRawConstPtrs & outputs)
{
    NameSet seen;
    NameSet ambiguous;
    for (const auto * output : outputs)
        if (!seen.insert(output->result_name).second)
            ambiguous.insert(output->result_name);
    return ambiguous;
}

/// Translate a constraint on the outputs of `dag` into a constraint on its inputs.
NameSet remapNullRejectedColumnsThroughActions(const ActionsDAG & dag, const NameSet & null_rejected_columns)
{
    const auto duplicate = duplicateOutputNames(dag.getOutputs());

    NameSet produced_columns;
    NameSet result;
    for (const auto * output : dag.getOutputs())
    {
        produced_columns.insert(output->result_name);
        if (!null_rejected_columns.contains(output->result_name) || duplicate.contains(output->result_name))
            continue;
        for (const auto * input : collectNullPropagatingInputs(output))
            result.insert(input->result_name);
    }

    /// `ActionsDAG::updateHeader` forwards an input column the DAG does not consume to the output header.
    NameSet consumed_columns;
    for (const auto * input : dag.getInputs())
        consumed_columns.insert(input->result_name);

    for (const auto & name : null_rejected_columns)
        if (!produced_columns.contains(name) && !consumed_columns.contains(name))
            result.insert(name);

    return result;
}

void collectNullRejectedColumnsFromFilter(const FilterStep & filter, NameSet & null_rejected_columns)
{
    const auto & dag = filter.getExpression();
    const auto * predicate = dag.tryFindInOutputs(filter.getFilterColumnName());
    if (!predicate)
        return;

    for (const auto * input : collectNullRejectedInputs(predicate))
        null_rejected_columns.insert(input->result_name);
}

bool addNotNullFilterAboveRead(QueryPlan::Node & node, const NameSet & null_rejected_columns, const FilterStep * parent_filter, double min_null_ratio, QueryPlan::Nodes & nodes)
{
    if (null_rejected_columns.empty())
        return false;

    const auto * read = typeid_cast<const ReadFromMergeTree *>(node.step.get());
    if (!read)
        return false;

    /// A conjunct that already rejects NULL there makes the derived one redundant.
    NameSet already_rejected;
    if (parent_filter)
        collectNullRejectedColumnsFromFilter(*parent_filter, already_rejected);

    const auto & header = *node.step->getOutputHeader();
    NameSet candidates;
    for (const auto & name : null_rejected_columns)
    {
        if (const auto * column = header.findByName(name); column && isNullableOrLowCardinalityNullable(column->type) && !already_rejected.contains(name))
            candidates.insert(name);
    }

    if (candidates.empty())
        return false;

    /// Add an `indexHint` if the null fraction of the column does not qualify it for an executable
    /// filter. This enables index analysis without the potential cost of running the filter.
    const auto [columns_to_filter, columns_to_index_hint] = [&] -> std::pair<NameSet, NameSet>
    {
        if (min_null_ratio <= 0.0)
            return {candidates, {}};

        auto estimator = read->getConditionSelectivityEstimator(Names(candidates.begin(), candidates.end()));
        if (!estimator)
            return {{}, candidates};

        NameSet to_filter;
        NameSet to_index_hint;
        const auto & column_stats = estimator->estimateRelationProfile().column_stats;
        for (const auto & name : candidates)
        {
            const auto it = column_stats.find(name);
            if (it != column_stats.end() && it->second.null_fraction && *it->second.null_fraction >= min_null_ratio)
                to_filter.insert(name);
            else
                to_index_hint.insert(name);
        }

        return {std::move(to_filter), std::move(to_index_hint)};
    }();

    auto context = Context::getGlobalContextInstance();
    auto is_not_null = FunctionFactory::instance().get("isNotNull", context);

    auto buildNotNullFilterOn = [&](ActionsDAG & target, const NameSet & columns)
    {
        ActionsDAG::NodeRawConstPtrs result;
        for (const auto * input : target.getInputs())
            if (columns.contains(input->result_name))
                result.push_back(&target.addFunction(is_not_null, {input}, {}));
        return result;
    };

    ActionsDAG filter_dag(header.getColumnsWithTypeAndName());
    auto filter_conjuncts = buildNotNullFilterOn(filter_dag, columns_to_filter);

    if (!columns_to_index_hint.empty())
    {
        ActionsDAG index_hint_dag(header.getColumnsWithTypeAndName());
        index_hint_dag.getOutputs() = buildNotNullFilterOn(index_hint_dag, columns_to_index_hint);
        auto index_hint_name = fmt::format("indexHint({})", fmt::join(index_hint_dag.getOutputs()
            | std::views::transform([](const auto * output) -> const String & { return output->result_name; }), ", "));
        auto index_hint = std::make_shared<FunctionIndexHint>();
        index_hint->setActions(std::move(index_hint_dag));
        filter_conjuncts.push_back(&filter_dag.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(index_hint)), {}, std::move(index_hint_name)));
    }

    if (filter_conjuncts.empty())
        return false;

    const ActionsDAG::Node * filter_node = filter_conjuncts.front();
    if (filter_conjuncts.size() > 1)
        filter_node = &filter_dag.addFunction(FunctionFactory::instance().get("and", context), std::move(filter_conjuncts), {});
    filter_dag.addOrReplaceInOutputs(*filter_node);

    return makeFilterNodeOnTopOf(node, std::move(filter_dag), filter_node->result_name, /*remove_filer=*/true, nodes, makeDescription("Derived NOT NULL filter and index hint"));
}

void collectNullRejectedColumnsFromJoinConditions(const JoinStepLogical & join, NameSet & left, NameSet & right)
{
    const auto & actions = join.getActionsDAG();
    if (actions.hasStatefulFunctions() || dagContainsNonDeterministicFunction(actions))
        return;

    const auto & join_operator = join.getJoinOperator();
    /// The sides for which a null-rejecting condition proves the input column not NULL.
    auto [discards_left, discards_right] = [&]() -> std::pair<bool, bool>
    {
        const auto kind = join_operator.kind;
        switch (join_operator.strictness)
        {
            case JoinStrictness::Unspecified:
            case JoinStrictness::All:
            case JoinStrictness::Any:
            case JoinStrictness::Anti:
                return {isInnerOrRight(kind), isInnerOrLeft(kind)};
            case JoinStrictness::Semi:
                return {isLeftOrRight(kind), isLeftOrRight(kind)};
            default:
                return {false, false};
        }
    }();
    if (!discards_left && !discards_right)
        return;

    auto is_null_rejecting_operator = [](JoinConditionOperator op)
    {
        switch (op)
        {
            case JoinConditionOperator::Equals:
            case JoinConditionOperator::Less:
            case JoinConditionOperator::LessOrEquals:
            case JoinConditionOperator::Greater:
            case JoinConditionOperator::GreaterOrEquals:
                return true;
            default:
                return false;
        }
    };

    for (const auto & expression : join_operator.expression)
    {
        auto [op, lhs, rhs] = expression.asBinaryPredicate();
        if (!is_null_rejecting_operator(op))
            continue;

        for (const auto & operand : {lhs, rhs})
        {
            if (!operand)
                continue;

            NameSet * side = nullptr;
            if (discards_left && operand.fromLeft())
                side = &left;
            else if (discards_right && operand.fromRight())
                side = &right;

            if (!side)
                continue;

            for (const auto * input : collectNullPropagatingInputs(operand.getNode()))
                side->insert(input->result_name);
        }
    }
}

/// Split a constraint on the join's output header between its two inputs.
std::pair<NameSet, NameSet> splitNullRejectedColumnsOnJoin(const JoinStepLogical & join, const NameSet & null_rejected_columns)
{
    NameSet left;
    NameSet right;
    if (null_rejected_columns.empty())
        return {std::move(left), std::move(right)};

    const auto & outputs = join.getActionsDAG().getOutputs();
    const auto duplicate = duplicateOutputNames(outputs);

    for (const auto * output : outputs)
    {
        if (!null_rejected_columns.contains(output->result_name) || duplicate.contains(output->result_name))
            continue;

        /// Only a column this join pads with a real NULL can witness that the padded rows are gone.
        /// `addToNullableIfNeeded` wraps exactly the padded columns in `toNullable`, and a column that
        /// is `Nullable` already is padded with its type default, which is NULL.
        if (!isNullableOrLowCardinalityNullable(output->result_type))
            continue;

        JoinActionRef ref(output, join.getExpressionActions());
        if (ref.fromLeft())
            left.insert(output->result_name);
        else if (ref.fromRight())
            right.insert(output->result_name);
    }
    return {std::move(left), std::move(right)};
}

void convertJoinKind(JoinStepLogical & join, QueryPlan::Node & node, const NameSet & left_null_rejected_columns, const NameSet & right_null_rejected_columns)
{
    auto & join_operator = join.getJoinOperator();
    const auto kind = join_operator.kind;
    if (kind != JoinKind::Left && kind != JoinKind::Right && kind != JoinKind::Full)
        return;

    if (join_operator.strictness != JoinStrictness::All)
        return;

    /// A `Join` engine source expects its declared join kind, and only an outer join on a key-value
    /// source must not be converted to prevent the optimizer from reordering it.
    auto is_prepared_join_storage = [&]()
    {
        for (const auto * child : node.children)
        {
            while (!typeid_cast<JoinStepLogicalLookup *>(child->step.get()) && child->children.size() == 1)
                child = child->children.front();

            if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
                if (lookup_step->getPreparedJoinStorage())
                    return true;
        }
        return false;
    }();
    if (is_prepared_join_storage)
        return;

    /// A side is "safe" when the rows this join would null-extend on it cannot survive above.
    const bool left_stream_safe = !left_null_rejected_columns.empty();
    const bool right_stream_safe = !right_null_rejected_columns.empty();

    if (kind == JoinKind::Full)
    {
        if (left_stream_safe && right_stream_safe)
            join_operator.kind = JoinKind::Inner;
        else if (left_stream_safe)
            join_operator.kind = JoinKind::Left;
        else if (right_stream_safe)
            join_operator.kind = JoinKind::Right;
    }
    else if (kind == JoinKind::Left && right_stream_safe)
        join_operator.kind = JoinKind::Inner;
    else if (kind == JoinKind::Right && left_stream_safe)
        join_operator.kind = JoinKind::Inner;
}

bool visit(QueryPlan::Node & root, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    struct Frame
    {
        QueryPlan::Node * node;
        NameSet null_rejected_columns;
        /// The `FilterStep` directly above this node.
        const FilterStep * parent_filter = nullptr;
    };

    const bool convert_joins = settings.convert_outer_join_to_inner_join_transitively;
    const bool derive_filters = settings.derive_not_null_filter_at_read;
    bool filters_added = false;

    std::vector<Frame> stack;
    stack.push_back({&root, {}, nullptr});

    while (!stack.empty())
    {
        auto frame = std::move(stack.back());
        stack.pop_back();

        auto & node = *frame.node;
        auto & null_rejected_columns = frame.null_rejected_columns;

        if (!node.step)
            continue;

        if (!node.step->hasOutputHeader())
        {
            for (auto * child : node.children)
                stack.push_back({child, {}, nullptr});
            continue;
        }

        /// Keep only the names this header carries exactly once.
        std::unordered_map<String, size_t> occurrences;
        for (const auto & column : *node.step->getOutputHeader())
            ++occurrences[column.name];
        std::erase_if(null_rejected_columns, [&](const auto & name) { return occurrences[name] != 1; });

        if (node.children.empty())
        {
            if (derive_filters)
                filters_added |= addNotNullFilterAboveRead(node, null_rejected_columns, frame.parent_filter, settings.derive_not_null_filter_at_read_min_null_ratio, nodes);
            continue;
        }

        const auto & step = *node.step;

        if (auto * join = typeid_cast<JoinStepLogical *>(node.step.get()); join && node.children.size() == 2)
        {
            if (isPaste(join->getJoinOperator().kind))
            {
                stack.push_back({node.children[0], {}});
                stack.push_back({node.children[1], {}});
                continue;
            }

            auto [left, right] = splitNullRejectedColumnsOnJoin(*join, null_rejected_columns);

            if (join->getJoinOperator().strictness == JoinStrictness::Asof)
                right.clear();

            if (convert_joins)
                convertJoinKind(*join, node, left, right);

            /// NULLs on a side this join can still null-extend may be introduced here, so a constraint
            /// observed above it is not guaranteed below it.
            const auto kind = join->getJoinOperator().kind;
            if (kind == JoinKind::Left || kind == JoinKind::Full)
                right.clear();
            if (kind == JoinKind::Right || kind == JoinKind::Full)
                left.clear();

            collectNullRejectedColumnsFromJoinConditions(*join, left, right);

            stack.push_back({node.children[0], std::move(left)});
            stack.push_back({node.children[1], std::move(right)});
            continue;
        }

        if (const auto * filter = typeid_cast<const FilterStep *>(&step))
        {
            if (filter->getExpression().hasStatefulFunctions() || dagContainsNonDeterministicFunction(filter->getExpression()))
            {
                stack.push_back({node.children.front(), {}});
                continue;
            }

            auto child_null_rejected_columns = remapNullRejectedColumnsThroughActions(filter->getExpression(), null_rejected_columns);
            collectNullRejectedColumnsFromFilter(*filter, child_null_rejected_columns);
            stack.push_back({node.children.front(), std::move(child_null_rejected_columns), filter});
            continue;
        }

        if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        {
            if (expression->getExpression().hasStatefulFunctions() || dagContainsNonDeterministicFunction(expression->getExpression()))
            {
                stack.push_back({node.children.front(), {}});
                continue;
            }

            stack.push_back({node.children.front(), remapNullRejectedColumnsThroughActions(expression->getExpression(), null_rejected_columns)});
            continue;
        }

        auto filterNullRejectedColumnsBy = [&](const Names & allowed)
        {
            NameSet result;
            for (const auto & name : allowed)
                if (null_rejected_columns.contains(name))
                    result.insert(name);
            return result;
        };

        /// The conditions below for propagating null rejected columns to inputs are a mirror
        /// of the conditions in `tryPushDownFilter` in `filterPushDown.cpp`.

        /// Only the grouping keys, because a constraint on them deletes the whole group.
        /// Under `GROUPING SETS` only the keys every set uses, because the others are emitted as NULL.
        auto groupingKeys = [&](const Names & keys, const GroupingSetsParamsList & grouping_sets)
        {
            auto result = filterNullRejectedColumnsBy(keys);
            for (const auto & grouping_set : grouping_sets)
                std::erase_if(result, [&](const auto & name) { return std::ranges::find(grouping_set.used_keys, name) == grouping_set.used_keys.end(); });
            return result;
        };

        if (const auto * aggregating = typeid_cast<const AggregatingStep *>(&step))
        {
            auto keys = aggregating->isGroupByUseNulls() ? NameSet{} : groupingKeys(aggregating->getParams().keys, aggregating->getGroupingSetsParamsList());
            stack.push_back({node.children.front(), std::move(keys)});
            continue;
        }

        if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(&step))
        {
            stack.push_back({node.children.front(), groupingKeys(merging_aggregated->getParams().keys, merging_aggregated->getGroupingSetsParamsList())});
            continue;
        }

        /// Only the PARTITION BY columns: dropping a whole partition changes no window value on a surviving row.
        if (const auto * window = typeid_cast<const WindowStep *>(&step))
        {
            Names partition_keys;
            for (const auto & sort_column : window->getWindowDescription().partition_by)
                partition_keys.push_back(sort_column.column_name);
            stack.push_back({node.children.front(), filterNullRejectedColumnsBy(partition_keys)});
            continue;
        }

        /// Only the LIMIT BY keys, and only when the step cannot empty a non-empty group.
        if (const auto * limit_by = typeid_cast<const LimitByStep *>(&step))
        {
            bool can_empty_group = limit_by->getGroupOffset() != 0 || limit_by->getGroupLength() == 0;
            stack.push_back({node.children.front(), can_empty_group ? NameSet{} : filterNullRejectedColumnsBy(limit_by->getColumns())});
            continue;
        }

        if (typeid_cast<const CreatingSetsStep *>(&step))
        {
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            for (size_t i = 1; i < node.children.size(); ++i)
                stack.push_back({node.children[i], {}});
            continue;
        }

        /// Everything but the array-join columns.
        if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(&step))
        {
            for (const auto & name : array_join->getColumns())
                null_rejected_columns.erase(name);
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            continue;
        }

        if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
        {
            stack.push_back({node.children.front(), sorting->getLimit() ? NameSet{} : std::move(null_rejected_columns)});
            continue;
        }

        if (const auto * distinct = typeid_cast<const DistinctStep *>(&step))
        {
            stack.push_back({node.children.front(), distinct->getLimitHint() ? NameSet{} : std::move(null_rejected_columns)});
            continue;
        }

        /// Passthrough steps.
        if (typeid_cast<const DelayedCreatingSetsStep *>(&step)
            || typeid_cast<const BuildRuntimeFilterStep *>(&step)
            || typeid_cast<const CreateSetAndFilterOnTheFlyStep *>(&step))
        {
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            continue;
        }

        /// Branches match the output by position, so a name carries over only when every branch header
        /// is structurally equal to the output.
        if (const auto * union_step = typeid_cast<const UnionStep *>(&step))
        {
            const auto & union_output = *union_step->getOutputHeader();
            bool all_branches_match = std::ranges::all_of(
                union_step->getInputHeaders(),
                [&](const auto & input_header) { return blocksHaveEqualStructure(*input_header, union_output); });

            for (auto * child : node.children)
                stack.push_back({child, all_branches_match ? null_rejected_columns : NameSet{}});
            continue;
        }

        /// Unrecognized step.
        for (auto * child : node.children)
            stack.push_back({child, {}});
    }

    return filters_added;
}

}

bool convertOuterToInnerAndAddNotNullFilters(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!optimization_settings.optimize_plan)
        return false;

    const bool convert_joins = optimization_settings.convert_outer_join_to_inner_join_transitively;
    const bool derive_filters = optimization_settings.derive_not_null_filter_at_read;
    if (!convert_joins && !derive_filters)
        return false;

    return visit(root, nodes, optimization_settings);
}

}
