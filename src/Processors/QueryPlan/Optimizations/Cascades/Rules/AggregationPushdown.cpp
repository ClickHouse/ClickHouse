#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Core/Joins.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <memory>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The join under the matched aggregation, optionally reached through one identity
/// `ExpressionStep` (e.g. `Expression (Before GROUP BY)`), which the transformation peels.
struct MatchedJoin
{
    GroupExpressionPtr join_expression;
    const JoinStepLogical * join_step = nullptr;
    const ExpressionStep * peeled_expression = nullptr;
};

/// L-side and R-side `INPUT` columns of the join's expression DAG reachable from the
/// `JoinOperator::expression` and `residual_filter` nodes.
struct ConditionInputs
{
    Names left;
    Names right;
};

/// Names of the join's projected output columns that are plain `INPUT` nodes, by the DAG's own
/// side attribution. A computed output (or one with unclear attribution) binds to neither side.
struct JoinOutputBindings
{
    NameSet left;
    NameSet right;
};

/// Pushes an aggregation below a join (eager aggregation) as a cost-based alternative, gated by
/// `cascades_aggregation_pushdown`. Variant A (partial pushdown, shown for pushed side S = L;
/// push-right is the mirror image):
///
///     Aggregating(final, keys=G, aggs=F)      MergingAggregated(keys=G, only-merge F)
///             |                                            |
///           Join(L, R)                =>                 Join'(L', R)
///           /      \                                     /      \
///          L        R                     Aggregating(final=false,
///                                           keys=(G ∩ L) ∪ J_L, aggs=F)
///                                                 |
///                                                 L
///
/// where `J_L` is the set of L-side columns the join condition reads. Every row within one
/// partial group is indistinguishable to the join (all join-relevant columns of the pushed
/// side are group keys), so the join filters or duplicates whole groups uniformly, and merging
/// `m` duplicated copies of a state equals processing the underlying rows `m` times - exactly
/// what the original plan does.
///
/// Variant B (full pushdown): when every `GROUP BY` key comes from the pushed side, the join
/// condition reads no other pushed-side columns (`J_S ⊆ G`), and the join emits each pushed row
/// at most once (see `isFullPushdownAllowed`), the aggregation stays final below the join and
/// the rebuilt join itself becomes the alternative - no merge above. B strictly dominates A
/// there (same grouping, one step less), so only B is emitted when it is legal.
class AggregationPushdown : public IOptimizationRule
{
public:
    String getName() const override { return "AggregationPushdown"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;

private:
    GroupExpressionPtr buildPushdownAlternative(
        const GroupExpressionPtr & source_expression,
        const MatchedJoin & match,
        const ConditionInputs & condition_inputs,
        const JoinOutputBindings & output_bindings,
        JoinTableSide side,
        Memo & memo) const;
};

/// Which (kind, strictness, side) combinations the rule may push through; extend here.
/// The pushed side must never be default-extended by the join - a default-extended
/// `AggregateFunction` state is an empty state that silently contributes nothing where the
/// original plan counts rows - so push-left forbids `isRightOrFull` kinds and push-right
/// forbids `isLeftOrFull` (`Full`, `Cross`, `Comma` and `Paste` never qualify).
/// Per strictness (multiplicities verified against `processMatch` in `HashJoinMethodsImpl.h`):
/// - `All`: a pushed row is emitted once per matching other-side row (or once default-extended
///   for an outer kind) - a function of its join-condition columns only, which are all partial
///   group keys, so groups are duplicated uniformly and the merge above restores the result.
/// - `Any`/`Semi`/`Anti`: legal only when the strictness filters the pushed side itself
///   (kind == pushed side): each pushed row is then emitted at most once, kept or dropped by a
///   predicate on its join-condition columns only. `Inner` + `Any` is excluded: it emits at
///   most one row per join *key* (`setUsedOnce` claims the key for the first probe row), so a
///   group of `m` raw rows contributes one raw row originally but its whole `m`-row state when
///   pushed. `RightAny` (deduplicate right keys) is excluded for the same per-key reason, and
///   `Asof` resolves the closest match per individual row.
bool isPushdownAllowed(JoinKind kind, JoinStrictness strictness, JoinTableSide side)
{
    const JoinKind pushed_side_kind = side == JoinTableSide::Left ? JoinKind::Left : JoinKind::Right;
    if (kind != JoinKind::Inner && kind != pushed_side_kind)
        return false;
    switch (strictness)
    {
        case JoinStrictness::All:
            return true;
        case JoinStrictness::Any:
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
            return kind == pushed_side_kind;
        default:
            return false;
    }
}

/// Variant B additionally requires that the join can never emit a pushed row twice, so the
/// final states need no merge: exactly the `Any`/`Semi`/`Anti` with kind == pushed side
/// combinations above (at most one emission per pushed row). `All` duplicates rows per match
/// and always needs the merge. The caller also checks the structural conditions (every
/// `GROUP BY` key on the pushed side, `J_S ⊆ G`).
bool isFullPushdownAllowed(JoinKind kind, JoinStrictness strictness, JoinTableSide side)
{
    const JoinKind pushed_side_kind = side == JoinTableSide::Left ? JoinKind::Left : JoinKind::Right;
    return kind == pushed_side_kind
        && (strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti);
}

/// Resolves the join under the aggregation, descending through at most one single-input
/// `ExpressionStep`. Looks only at `front()` of the child groups - the ingested plan; other
/// logical alternatives (e.g. the swapped join) would only produce duplicate memo groups.
/// Input links with non-empty required properties carry a stripped `Sort`, which the
/// transformation would silently drop - such shapes do not match.
std::optional<MatchedJoin> resolveJoinUnderAggregation(const GroupExpression & expression, const Memo & memo)
{
    if (expression.inputs.size() != 1)
        return {};
    if (!(expression.inputs[0].required_properties == ExpressionProperties{}))
        return {};

    MatchedJoin match;
    match.join_expression = memo.getGroup(expression.inputs[0].group_id)->logical_expressions.front();

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(match.join_expression->getQueryPlanStep()))
    {
        if (match.join_expression->inputs.size() != 1)
            return {};
        if (!(match.join_expression->inputs[0].required_properties == ExpressionProperties{}))
            return {};
        match.peeled_expression = expression_step;
        match.join_expression = memo.getGroup(match.join_expression->inputs[0].group_id)->logical_expressions.front();
    }

    match.join_step = typeid_cast<const JoinStepLogical *>(match.join_expression->getQueryPlanStep());
    if (!match.join_step || match.join_expression->inputs.size() != 2)
        return {};
    return match;
}

bool AggregationPushdown::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step || expression->strategy != nullptr)
        return false;

    const auto & params = agg_step->getParams();
    if (!agg_step->getFinal() ||
        agg_step->isGroupingSets() ||           /// distributed merging of grouping-set states is not supported
        params.overflow_row ||
        params.max_rows_to_group_by != 0 ||     /// global row limit must be enforced by one aggregator
        params.only_merge ||                    /// don't split a merge step that's already from a prior split
        agg_step->inOrder() ||
        agg_step->explicitSortingRequired() ||
        params.bucket_top_k != 0)               /// bucket top-K assumes this aggregation directly feeds its `ORDER BY ... LIMIT`
        return false;

    /// Global aggregation is not safe: an empty join output yields one global row originally,
    /// but zero rows through partial + merge.
    if (params.keys.empty())
        return false;

    auto match = resolveJoinUnderAggregation(*expression, memo);
    if (!match)
        return false;

    const auto & join_operator = match->join_step->getJoinOperator();
    return isPushdownAllowed(join_operator.kind, join_operator.strictness, JoinTableSide::Left)
        || isPushdownAllowed(join_operator.kind, join_operator.strictness, JoinTableSide::Right);
}

/// Nullopt when a reachable `INPUT` cannot be attributed to exactly one side or is missing from
/// that side's header.
std::optional<ConditionInputs> collectConditionInputs(const JoinStepLogical & join_step)
{
    std::unordered_map<const ActionsDAG::Node *, JoinActionRef> input_refs;
    for (const auto & input : join_step.getInputActions())
        input_refs.emplace(input.getNode(), input);

    const auto & left_header = *join_step.getInputHeaders().front();
    const auto & right_header = *join_step.getInputHeaders().back();
    const auto & join_operator = join_step.getJoinOperator();

    std::vector<const ActionsDAG::Node *> stack;
    for (const auto & action : join_operator.expression)
        stack.push_back(action.getNode());
    for (const auto & action : join_operator.residual_filter)
        stack.push_back(action.getNode());

    ConditionInputs result;
    std::unordered_set<const ActionsDAG::Node *> visited;
    NameSet seen_left;
    NameSet seen_right;
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            auto it = input_refs.find(node);
            if (it == input_refs.end())
                return {};
            if (it->second.fromLeft() && left_header.has(node->result_name))
            {
                if (seen_left.insert(node->result_name).second)
                    result.left.push_back(node->result_name);
            }
            else if (it->second.fromRight() && right_header.has(node->result_name))
            {
                if (seen_right.insert(node->result_name).second)
                    result.right.push_back(node->result_name);
            }
            else
                return {};
        }

        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return result;
}

/// Defense-in-depth for the name-based side resolution: every column the pushdown consumes
/// from the join (`GROUP BY` keys, aggregate arguments) must be a projected `INPUT` node of the
/// expected side, not a computed column that merely shadows an input name.
JoinOutputBindings collectOutputBindings(const JoinStepLogical & join_step)
{
    JoinOutputBindings result;
    for (const auto & action : join_step.getOutputActions())
    {
        const auto * node = action.getNode();
        if (node->type != ActionsDAG::ActionType::INPUT)
            continue;
        if (action.fromLeft())
            result.left.insert(node->result_name);
        else if (action.fromRight())
            result.right.insert(node->result_name);
    }
    return result;
}

void remapNodes(ActionsDAG::NodeRawConstPtrs & nodes, const ActionsDAG::NodeMapping & node_map)
{
    for (const auto *& node : nodes)
    {
        if (auto it = node_map.find(node); it != node_map.end())
            node = it->second;
    }
}

/// Rebuilds the join with the pushed side's input replaced by `new_pushed_header` (the partial
/// aggregation's output) and the output narrowed to `output_names`. The join condition sub-DAG
/// is cloned into a fresh DAG over the new input headers; the caller guarantees all its `INPUT`
/// columns exist there under the same names and types. Returns nullptr when an internal
/// invariant does not hold - the caller then emits nothing.
std::unique_ptr<JoinStepLogical> rebuildJoinWithNewInput(
    const JoinStepLogical & join_step,
    size_t pushed_input_index,
    const SharedHeader & new_pushed_header,
    const Names & output_names)
{
    const auto & join_operator = join_step.getJoinOperator();

    ActionsDAG::NodeRawConstPtrs cond_nodes;
    for (const auto & action : join_operator.expression)
        cond_nodes.push_back(action.getNode());
    const size_t expression_size = cond_nodes.size();
    for (const auto & action : join_operator.residual_filter)
        cond_nodes.push_back(action.getNode());

    SharedHeader new_left_header = pushed_input_index == 0 ? new_pushed_header : join_step.getInputHeaders().front();
    SharedHeader new_right_header = pushed_input_index == 1 ? new_pushed_header : join_step.getInputHeaders().back();

    auto all_columns = new_left_header->getColumnsWithTypeAndName();
    const auto right_columns = new_right_header->getColumnsWithTypeAndName();
    all_columns.insert(all_columns.end(), right_columns.begin(), right_columns.end());

    /// Inputs == outputs in header order, so the merged-in condition inputs match by name.
    ActionsDAG dag(all_columns, /*duplicate_const_columns=*/false);

    ActionsDAG::NodeMapping clone_map;
    auto condition_subdag = ActionsDAG::cloneSubDAG(cond_nodes, clone_map, false);
    remapNodes(cond_nodes, clone_map);

    ActionsDAG::NodeMapping merge_map;
    dag.mergeInplace(std::move(condition_subdag), merge_map, /*remove_dangling_inputs=*/true);
    remapNodes(cond_nodes, merge_map);

    /// A leftover input means a condition column missing from the new headers.
    if (dag.getInputs().size() != all_columns.size())
        return nullptr;

    std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs_by_name;
    for (const auto * input : dag.getInputs())
        inputs_by_name.emplace(input->result_name, input);

    ActionsDAG::NodeRawConstPtrs output_nodes;
    output_nodes.reserve(output_names.size());
    for (const auto & name : output_names)
    {
        auto it = inputs_by_name.find(name);
        if (it == inputs_by_name.end())
            return nullptr;
        output_nodes.push_back(it->second);
    }
    dag.getOutputs() = output_nodes;

    JoinExpressionActions new_actions(*new_left_header, *new_right_header, std::move(dag));

    JoinOperator new_operator(join_operator.kind, join_operator.strictness, join_operator.locality);
    for (size_t i = 0; i < cond_nodes.size(); ++i)
    {
        auto action = JoinActionRef(cond_nodes[i], new_actions);
        if (i < expression_size)
            new_operator.expression.push_back(std::move(action));
        else
            new_operator.residual_filter.push_back(std::move(action));
    }
    /// Key names and values are preserved by construction, so runtime filters stay correct.
    new_operator.shared_runtime_filter_descriptors = join_operator.shared_runtime_filter_descriptors;

    auto new_join_step = std::make_unique<JoinStepLogical>(
        new_left_header,
        new_right_header,
        std::move(new_operator),
        std::move(new_actions),
        std::move(output_nodes),
        join_step.getJoinSettings(),
        join_step.getSortingSettings());
    /// Pin the join layout, but leave `result_rows_estimation` empty: the old estimate
    /// describes the un-aggregated join and would poison `deriveJoinStatistics`.
    new_join_step->setOptimized();
    new_join_step->setStepDescription(join_step);
    return new_join_step;
}

GroupExpressionPtr AggregationPushdown::buildPushdownAlternative(
    const GroupExpressionPtr & source_expression,
    const MatchedJoin & match,
    const ConditionInputs & condition_inputs,
    const JoinOutputBindings & output_bindings,
    JoinTableSide side,
    Memo & memo) const
{
    const auto & agg_step = *typeid_cast<const AggregatingStep *>(source_expression->getQueryPlanStep());
    const auto & join_step = *match.join_step;
    const auto & params = agg_step.getParams();

    const size_t pushed_input_index = side == JoinTableSide::Left ? 0 : 1;
    const auto & pushed_header = *join_step.getInputHeaders()[pushed_input_index];
    const auto & other_header = *join_step.getInputHeaders()[1 - pushed_input_index];
    const Names & pushed_condition_inputs = side == JoinTableSide::Left ? condition_inputs.left : condition_inputs.right;
    const NameSet & pushed_bindings = side == JoinTableSide::Left ? output_bindings.left : output_bindings.right;
    const NameSet & other_bindings = side == JoinTableSide::Left ? output_bindings.right : output_bindings.left;

    /// Every aggregate argument must come from the pushed side, and bind to a projected `INPUT`
    /// of that side in the join's own DAG.
    for (const auto & aggregate : params.aggregates)
        for (const auto & argument_name : aggregate.argument_names)
            if (!pushed_header.has(argument_name) || other_header.has(argument_name) || !pushed_bindings.contains(argument_name))
                return nullptr;

    /// Every `GROUP BY` key must resolve to exactly one side, confirmed by the join DAG's own
    /// attribution. Other-side keys stay keys of the top merge and must be among the join's
    /// projected outputs. Duplicate keys would break the positional layout the merge relies on
    /// (see below) - bail out.
    Names pushed_keys;
    NameSet pushed_key_set;
    NameSet other_key_set;
    for (const auto & key : params.keys)
    {
        const bool in_pushed = pushed_header.has(key);
        const bool in_other = other_header.has(key);
        if (in_pushed == in_other)
            return nullptr;
        if (!(in_pushed ? pushed_bindings : other_bindings).contains(key))
            return nullptr;
        if (in_pushed)
        {
            if (!pushed_key_set.insert(key).second)
                return nullptr;
            pushed_keys.push_back(key);
        }
        else if (!other_key_set.insert(key).second)
            return nullptr;
    }

    /// The pushed side's `GROUP BY` keys become the partial keys, extended with the pushed
    /// side's join-condition columns (which participate in the condition but are not projected).
    bool condition_extends_keys = false;
    for (const auto & name : pushed_condition_inputs)
        if (pushed_key_set.insert(name).second)
        {
            pushed_keys.push_back(name);
            condition_extends_keys = true;
        }

    /// Variant B: every `GROUP BY` key is on the pushed side and the join condition reads only
    /// `GROUP BY` keys of it, so the aggregation stays final below the join and no merge is
    /// needed above. B strictly dominates A here (same grouping, no merge) - emit only B.
    const auto & join_operator = join_step.getJoinOperator();
    const bool full_pushdown = other_key_set.empty() && !condition_extends_keys
        && isFullPushdownAllowed(join_operator.kind, join_operator.strictness, side);

    /// The rebuilt pushed-side header (keys + aggregate state columns) must have unique names
    /// disjoint from the other side's - `JoinExpressionActions` requires it.
    NameSet new_pushed_names = pushed_key_set;
    for (const auto & aggregate : params.aggregates)
        if (!new_pushed_names.insert(aggregate.column_name).second)
            return nullptr;
    for (const auto & name : new_pushed_names)
        if (other_header.has(name))
            return nullptr;

    /// A join input header may carry null column objects (e.g. when `preCalculateKeys` rebuilt
    /// it from DAG result columns); the aggregation header math materializes columns, so give
    /// every column a real (empty) object.
    auto partial_input_header = std::make_shared<Block>();
    for (const auto & column : pushed_header)
        partial_input_header->insert({column.column ? column.column : column.type->createColumn(), column.type, column.name});

    auto partial_step = cloneStepAs(agg_step);
    if (!full_pushdown)
        partial_step->setFinal(false);
    /// Unlike `TwoStageAggregationTransformation`, no bucket-order forcing under the
    /// memory-efficient merge: bucket annotations do not survive the join anyway, and the merge
    /// treats the join output as single-level data.
    partial_step->rebaseOntoInput(std::move(partial_input_header), pushed_keys);
    partial_step->setStepDescription(
        fmt::format("{}: {}", full_pushdown ? "Pushed" : "Partial", agg_step.getStepDescription()), 200);

    /// The join condition columns must keep their types through the pushed aggregation,
    /// otherwise the rebuilt condition DAG would silently mix types.
    const auto & partial_header = *partial_step->getOutputHeader();
    for (const auto & name : pushed_condition_inputs)
        if (!partial_header.getByName(name).type->equals(*pushed_header.getByName(name).type))
            return nullptr;

    /// The rebuilt join projects exactly the merge's expected positional layout: all `GROUP BY`
    /// keys in `params.keys` order (from whichever side each comes), then the aggregate columns
    /// in `params.aggregates` order. The `only_merge` aggregator reads keys and states by
    /// position (see `calculateKeysPositions`), so this order is load-bearing for variant A;
    /// for variant B (all columns from the pushed side) it equals `params.getHeader` order.
    Names output_names = params.keys;
    for (const auto & aggregate : params.aggregates)
        output_names.push_back(aggregate.column_name);

    auto new_join_step = rebuildJoinWithNewInput(join_step, pushed_input_index, partial_step->getOutputHeader(), output_names);
    if (!new_join_step)
        return nullptr;

    /// The alternative must produce the aggregation's exact output columns (order is cosmetic -
    /// plan extraction inserts name-based converting expressions). On any mismatch, emit nothing.
    const auto & original_header = *agg_step.getOutputHeader();
    const auto check_header = [&original_header](const Block & alternative_header)
    {
        if (alternative_header.columns() != original_header.columns())
            return false;
        for (const auto & column : original_header)
        {
            const auto * alternative_column = alternative_header.findByName(column.name);
            if (!alternative_column || !alternative_column->type->equals(*column.type))
                return false;
        }
        return true;
    };

    if (full_pushdown)
    {
        if (!check_header(*new_join_step->getOutputHeader()))
            return nullptr;

        GroupExpressionPtr pushed_expression = std::make_shared<GroupExpression>(std::move(partial_step));
        GroupExpressionPtr join_alternative = std::make_shared<GroupExpression>(std::move(new_join_step));
        return addEagerAggregationFullPushdown(memo, source_expression, match.join_expression, pushed_input_index,
            std::move(pushed_expression), std::move(join_alternative));
    }

    auto merge_params = agg_step.getParams();
    merge_params.only_merge = true;
    auto merge_step = std::make_unique<MergingAggregatedStep>(
        new_join_step->getOutputHeader(),
        std::move(merge_params),
        agg_step.getGroupingSetsParamsList(),
        /*final_=*/true,
        memo.getEnvironment().distributed_aggregation_memory_efficient,
        agg_step.getTemporaryDataMergeThreads(),
        agg_step.shouldProduceResultsInBucketOrder(),
        agg_step.getMaxBlockSize(),
        agg_step.getMaxBlockSizeForAggregationInOrder(),
        agg_step.usingMemoryBoundMerging());
    /// The join between the partial aggregation and this merge does not annotate chunks with
    /// `AggregatedChunkInfo`; un-annotated state data is treated as single-level.
    merge_step->allowInputWithoutAggregatedChunkInfo();
    merge_step->setStepDescription(fmt::format("Merge: {}", agg_step.getStepDescription()), 200);

    if (!check_header(*merge_step->getOutputHeader()))
        return nullptr;

    GroupExpressionPtr partial_expression = std::make_shared<GroupExpression>(std::move(partial_step));
    GroupExpressionPtr join_alternative = std::make_shared<GroupExpression>(std::move(new_join_step));
    return addEagerAggregationSplit(memo, source_expression, match.join_expression, pushed_input_index,
        std::move(partial_expression), std::move(join_alternative), std::move(merge_step));
}

std::vector<GroupExpressionPtr> AggregationPushdown::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationPushdown::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());

    auto match = resolveJoinUnderAggregation(*expression, memo);
    if (!match)
        return {};

    const auto & join_step = *match->join_step;

    /// `join_use_nulls` type changes and `USING` casts are excluded: an `AggregateFunction`
    /// state column cannot be made `Nullable`.
    if (!join_step.typeChangingSides().empty())
        return {};

    /// When an `ExpressionStep` sits between the aggregation and the join, every needed column
    /// must trace to an identically-named join output; the transformation then just drops the
    /// step. A computed column makes the rule inapplicable.
    if (match->peeled_expression)
    {
        Names needed_names = agg_step->getParams().keys;
        for (const auto & aggregate : agg_step->getParams().aggregates)
            needed_names.insert(needed_names.end(), aggregate.argument_names.begin(), aggregate.argument_names.end());

        const auto & peeled_dag = match->peeled_expression->getExpression();
        for (const auto & name : needed_names)
        {
            String input_name;
            if (classifyOutputName(peeled_dag, name, input_name) != TranslatedName::Traced || input_name != name)
                return {};
        }
    }

    auto condition_inputs = collectConditionInputs(join_step);
    if (!condition_inputs)
        return {};

    const auto output_bindings = collectOutputBindings(join_step);

    std::vector<GroupExpressionPtr> result;
    const auto & join_operator = join_step.getJoinOperator();
    for (const auto side : {JoinTableSide::Left, JoinTableSide::Right})
    {
        if (!isPushdownAllowed(join_operator.kind, join_operator.strictness, side))
            continue;
        if (auto alternative = buildPushdownAlternative(expression, *match, *condition_inputs, output_bindings, side, memo))
            result.push_back(std::move(alternative));
    }
    return result;
}

}

OptimizationRulePtr createAggregationPushdown() { return std::make_shared<AggregationPushdown>(); }

}
