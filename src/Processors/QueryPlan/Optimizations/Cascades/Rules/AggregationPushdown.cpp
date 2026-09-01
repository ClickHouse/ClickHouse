#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <algorithm>
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
/// `cascades_aggregation_pushdown`. Variant A, partial pushdown (shown for the left side;
/// push-right is the mirror image):
///
///     Aggregating(final, keys=G, aggs=F)      Aggregating(only_merge, keys=G, aggs=F)
///             |                                            |
///           Join(L, R)                =>                 Join'(L', R)
///           /      \                                     /      \
///          L        R                     Aggregating(final=false,
///                                           keys=G_L+J_L, aggs=F)
///                                                 |
///                                                 L
///
/// G_L are the L-side `GROUP BY` keys, J_L the L-side columns the join condition reads. All
/// join-relevant columns of L are then group keys, so the rows within one partial group are
/// indistinguishable to the join: it filters or duplicates whole groups uniformly, and merging
/// `m` copies of a state equals processing its rows `m` times - exactly the original plan.
/// Variant A is skipped under `distributed_plan_force_shuffle_aggregation`, which forbids
/// exactly this partial + merge split.
///
/// Variant B, full pushdown: when every `GROUP BY` key comes from the pushed side, the join
/// condition reads only `GROUP BY` keys, and the join emits each pushed row at most once (see
/// `isFullPushdownAllowed`), the aggregation stays final below the join with no merge above.
/// B dominates A there (same grouping, one step less), so only B is emitted when it is legal.
///
/// Bail-outs: a join condition that reads a function non-deterministic within the query (see
/// `collectConditionInputs`), and pushed keys without reliable distinct-value counts proving
/// the partial aggregation shrinks its input (see the cardinality gate in
/// `buildPushdownAlternative`; statistics are hint-only today, so a hint-less query never
/// takes this rewrite).
///
/// The join is matched against every logical alternative in the group below the aggregation,
/// not only the original plan (see `collectJoinsUnderAggregation`), so the rule also fires on
/// e.g. `JoinCommutativity`'s swapped twin; the twin's pushdown is a mirror image of the
/// original's and currently lands in a duplicate memo group. Engine limits: `checkPattern`
/// runs before the child group is explored, and each rule runs once per source expression, so
/// a join that appears in the child group only later (e.g. from a nested aggregation's own
/// pushdown) never triggers the rule.
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

/// Turns a `JoinStepLogical` group expression into a candidate, appending it to `result` unless
/// its `join_expression` pointer was already collected (the same join reachable via more than
/// one identity-expression path above it).
void addJoinCandidate(std::vector<MatchedJoin> & result, GroupExpressionPtr join_expression, const ExpressionStep * peeled_expression)
{
    for (const auto & existing : result)
        if (existing.join_expression == join_expression)
            return;

    const auto * join_step = typeid_cast<const JoinStepLogical *>(join_expression->getQueryPlanStep());
    if (!join_step || join_expression->inputs.size() != 2)
        return;

    MatchedJoin match;
    match.join_expression = std::move(join_expression);
    match.join_step = join_step;
    match.peeled_expression = peeled_expression;
    result.push_back(std::move(match));
}

/// Collects every join alternative reachable under the matched aggregation, descending through
/// at most one single-input `ExpressionStep`. A memo group is a set of logically-equivalent
/// alternatives, so the rule enumerates ALL `logical_expressions` of the child group (and, behind
/// an identity `ExpressionStep`, of the grandchild group) instead of looking only at `front()` -
/// the ingested plan. Child groups are fully explored before this rule runs on the parent
/// (`scheduleApplicableRules` schedules the child `ExploreGroupTask`s ahead of `ApplyRuleTask` on
/// the LIFO stack), so at this point the child group already carries e.g. `JoinCommutativity`'s
/// swapped twin and any join alternative a nested aggregation's own pushdown appended.
///
/// No alternative-level deduplication beyond the same-`join_expression`-pointer skip above: the
/// swapped twin is a distinct `JoinStepLogical` expression and is processed like any other,
/// producing a mirror-image pushdown that lands in a structurally-distinct duplicate memo group
/// (fresh input group ids defeat `Group::addLogicalExpression`'s structural dedup). This
/// duplication is accepted here; dedup of mirror alternatives is a known memo-level gap.
///
/// Input links with non-empty required properties carry a stripped `Sort`, which the
/// transformation would silently drop - such shapes do not match.
///
/// `stop_at_first_pushable` short-circuits (for `checkPattern`'s cheap existence check) as soon
/// as a collected candidate's join kind/strictness allows pushing to either side - no DAG walks,
/// just the coarse `isPushdownAllowed` test. `applyImpl` passes false to collect every candidate.
std::vector<MatchedJoin> collectJoinsUnderAggregation(const GroupExpression & expression, const Memo & memo, bool stop_at_first_pushable)
{
    std::vector<MatchedJoin> result;
    if (expression.inputs.size() != 1)
        return result;
    if (!(expression.inputs[0].required_properties == ExpressionProperties{}))
        return result;

    const auto is_pushable = [](const MatchedJoin & match)
    {
        const auto & join_operator = match.join_step->getJoinOperator();
        return isPushdownAllowed(join_operator.kind, join_operator.strictness, JoinTableSide::Left)
            || isPushdownAllowed(join_operator.kind, join_operator.strictness, JoinTableSide::Right);
    };

    for (const auto & child_expression : memo.getGroup(expression.inputs[0].group_id)->logical_expressions)
    {
        const size_t size_before = result.size();
        if (typeid_cast<const JoinStepLogical *>(child_expression->getQueryPlanStep()))
        {
            addJoinCandidate(result, child_expression, /*peeled_expression=*/nullptr);
        }
        else if (const auto * expression_step = typeid_cast<const ExpressionStep *>(child_expression->getQueryPlanStep()))
        {
            if (child_expression->inputs.size() != 1)
                continue;
            if (!(child_expression->inputs[0].required_properties == ExpressionProperties{}))
                continue;
            for (const auto & grandchild_expression : memo.getGroup(child_expression->inputs[0].group_id)->logical_expressions)
                if (typeid_cast<const JoinStepLogical *>(grandchild_expression->getQueryPlanStep()))
                    addJoinCandidate(result, grandchild_expression, expression_step);
        }

        if (stop_at_first_pushable)
            for (size_t i = size_before; i < result.size(); ++i)
                if (is_pushable(result[i]))
                    return {result[i]};
    }
    /// No pushable candidate found: in coarse mode, `result` may still hold non-pushable
    /// candidates collected along the way - those must not make `checkPattern` match.
    if (stop_at_first_pushable)
        return {};
    return result;
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

    /// Coarse existence check: match iff any join alternative under the aggregation has a join
    /// kind/strictness that allows pushing to either side. No DAG walks here - `applyImpl` runs
    /// the precise per-candidate legality checks.
    return !collectJoinsUnderAggregation(*expression, memo, /*stop_at_first_pushable=*/true).empty();
}

/// Nullopt when a reachable `INPUT` cannot be attributed to exactly one side or is missing from
/// that side's header, or when the condition reads a function non-deterministic in scope of the
/// query (e.g. `rand`).
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

        /// The multiplicity argument requires the condition to be a pure function of the pushed
        /// side's group keys; a function non-deterministic in scope of the query (`rand`)
        /// evaluates per joined row, and the rewrite would collapse those per-row evaluations to
        /// one per group. Same predicate as `dagContainsNonDeterministicFunction` in `Utils.cpp`.
        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && !node->function_base->isDeterministicInScopeOfQuery())
            return {};

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
    /// `right_hash_table_cache_key` and the relation estimates likewise stay empty (default):
    /// the originals describe the un-aggregated build side, and a wrong size hint is worse than
    /// none. Recomputing them the way the join reorderer does for new joins
    /// (`deriveCacheKeysForNewJoin`) would need reorderer-style subtree hashes, which do not
    /// exist over memo groups.
    new_join_step->setOptimized();
    new_join_step->setStepDescription(join_step);
    return new_join_step;
}

/// Registers the pushdown alternative in the memo: the pushed aggregation becomes its own group
/// over the join's pushed input, the rebuilt join a group over (pushed aggregation, other input).
/// With `merge_step` (variant A) the merge becomes the alternative in the source group; without
/// (variant B, full pushdown) the rebuilt join itself does. The top expression is marked with the
/// rule so the transformation is not re-applied to it.
GroupExpressionPtr registerPushdownAlternative(
    Memo & memo,
    const IOptimizationRule & rule,
    const GroupExpressionPtr & source_expression,
    const GroupExpressionPtr & join_expression,
    size_t pushed_input_index,
    GroupExpressionPtr pushed_aggregation_expression,
    GroupExpressionPtr join_alternative_expression,
    QueryPlanStepPtr merge_step) /// nullptr => full pushdown
{
    pushed_aggregation_expression->inputs = {join_expression->inputs[pushed_input_index]};
    GroupId aggregation_group_id = memo.addGroup(pushed_aggregation_expression);

    join_alternative_expression->inputs = join_expression->inputs;
    join_alternative_expression->inputs[pushed_input_index] = {aggregation_group_id, {}};

    GroupExpressionPtr top_expression;
    if (merge_step)
    {
        GroupId join_group_id = memo.addGroup(join_alternative_expression);
        top_expression = std::make_shared<GroupExpression>(std::move(merge_step));
        top_expression->inputs = {{join_group_id, {}}};
    }
    else
        top_expression = std::move(join_alternative_expression);

    top_expression->setApplied(rule, {});
    /// The pushed-aggregation group is new, so the top expression is a duplicate only when the
    /// same pushdown was already registered. Return nothing then, so the dropped expression is
    /// not explored.
    if (!memo.getGroup(source_expression->group_id)->addLogicalExpression(top_expression))
        return nullptr;
    return top_expression;
}

/// `deriveAggregatingStatistics` falls back to `0.1 * input_rows` for a key without stats -
/// a reasonable guess for pricing the ORIGINAL aggregation, but the pushdown widens the key set
/// to `(G ∩ S) ∪ J_S`, and the fallback would then price the widened set the same as `G` alone,
/// hiding the extra aggregation stage as if it cost nothing. Require a real NDV for every pushed
/// key instead.
bool pushedKeysHaveReliableCardinality(const ExpressionStatistics & input_statistics, const Names & pushed_keys)
{
    for (const auto & key : pushed_keys)
    {
        auto it = input_statistics.column_statistics.find(key);
        if (it == input_statistics.column_statistics.end() || it->second.num_distinct_values == 0)
            return false;
    }
    return true;
}

/// Minimum shrinkage the composite bound must prove (see `pushedKeysGuaranteeReduction`).
constexpr Float64 MIN_GUARANTEED_REDUCTION = 2.0;

/// The product of the pushed keys' NDVs (each clamped to the input estimate, keeping the whole
/// comparison in estimated-row space) is a proven upper bound on the partial's output row count.
/// The memo will later price the alternative optimistically, by the max of the keys'
/// NDVs rather than this composite, so requiring the composite to guarantee at least
/// `MIN_GUARANTEED_REDUCTION` below the input compensates: a key set that provably does not
/// shrink the input is rejected even though the max-of-NDVs estimate alone would look profitable.
bool pushedKeysGuaranteeReduction(const ExpressionStatistics & input_statistics, const Names & pushed_keys)
{
    Float64 composite = 1;
    for (const auto & key : pushed_keys)
    {
        const Float64 ndv = std::min(Float64(input_statistics.column_statistics.at(key).num_distinct_values), input_statistics.estimated_row_count);
        composite *= ndv;
        if (composite >= input_statistics.estimated_row_count)
        {
            composite = input_statistics.estimated_row_count;
            break;
        }
    }
    return composite * MIN_GUARANTEED_REDUCTION <= input_statistics.estimated_row_count;
}

/// The partial aggregation's key set and the classification behind it.
struct PushdownKeys
{
    Names pushed_keys;      /// pushed-side `GROUP BY` keys + the pushed side's join-condition columns
    NameSet pushed_key_set; /// `pushed_keys` as a set
    bool has_other_side_keys = false;
    bool condition_extends_keys = false;
};

/// Classifies the `GROUP BY` keys by join side and extends the pushed ones with the pushed
/// side's join-condition columns (which participate in the condition but are not projected).
/// Every key must resolve to exactly one side, confirmed by the join DAG's own attribution;
/// other-side keys stay keys of the top merge and must be among the join's projected outputs.
/// Nullopt on any violation, or on a duplicate key - duplicates would break the positional
/// layout the merge relies on (see `buildMergeStep`).
std::optional<PushdownKeys> classifyPushdownKeys(
    const Aggregator::Params & params,
    const Block & pushed_header,
    const Block & other_header,
    const NameSet & pushed_bindings,
    const NameSet & other_bindings,
    const Names & pushed_condition_inputs)
{
    PushdownKeys result;
    NameSet other_key_set;
    for (const auto & key : params.keys)
    {
        const bool in_pushed = pushed_header.has(key);
        const bool in_other = other_header.has(key);
        if (in_pushed == in_other)
            return {};
        if (!(in_pushed ? pushed_bindings : other_bindings).contains(key))
            return {};
        if (in_pushed)
        {
            if (!result.pushed_key_set.insert(key).second)
                return {};
            result.pushed_keys.push_back(key);
        }
        else if (!other_key_set.insert(key).second)
            return {};
    }
    result.has_other_side_keys = !other_key_set.empty();

    for (const auto & name : pushed_condition_inputs)
        if (result.pushed_key_set.insert(name).second)
        {
            result.pushed_keys.push_back(name);
            result.condition_extends_keys = true;
        }
    return result;
}

/// The positional layout the `only_merge` reader expects on its input (see
/// `calculateKeysPositions`): all `GROUP BY` keys in `params.keys` order, then the aggregate
/// columns in `params.aggregates` order.
bool headerIsKeysThenAggregates(const Block & header, const Aggregator::Params & params)
{
    if (header.columns() != params.keys.size() + params.aggregates.size())
        return false;
    for (size_t i = 0; i < params.keys.size(); ++i)
        if (header.getByPosition(i).name != params.keys[i])
            return false;
    for (size_t i = 0; i < params.aggregates.size(); ++i)
        if (header.getByPosition(params.keys.size() + i).name != params.aggregates[i].column_name)
            return false;
    return true;
}

/// Builds variant A's merge-only `AggregatingStep` over the rebuilt join's output. A merge-only
/// `AggregatingStep` rather than a `MergingAggregatedStep`: its `only_merge` consume path
/// (`mergeOnBlock`) reads keys and states positionally and never requires `AggregatedChunkInfo`
/// on input, so the rebuilt join's output needs no chunk annotation. The full `Params` copy
/// keeps the spill and two-level settings alive for the merge. Input-LAYOUT properties of the
/// original aggregation (the sort descriptions, the evenly-distributed-read flag) describe the
/// ORIGINAL input and must not be carried onto a merge over rebuilt join output; behavior
/// toggles are copied from the original step.
std::unique_ptr<AggregatingStep> buildMergeStep(const AggregatingStep & agg_step, const SharedHeader & input_header)
{
    auto merge_params = agg_step.getParams();
    merge_params.only_merge = true;
    /// Same reasoning as the partial step's `setStatsCacheKey(0)` in the caller: this merge
    /// consumes post-join states of a different shape than the original aggregation the key was
    /// stamped for, so a shared key would cross-contaminate `HashTablesStatistics`.
    merge_params.stats_collecting_params.setKey(0);

    /// The `only_merge` reader is positional; only the caller's construction order guarantees
    /// this layout (the caller's `check_header` compares names and types, not positions).
    chassert(headerIsKeysThenAggregates(*input_header, merge_params));

    auto merge_step = std::make_unique<AggregatingStep>(
        input_header,
        std::move(merge_params),
        GroupingSetsParamsList{}, /// `checkPattern` bails on grouping sets
        /*final_=*/true,
        agg_step.getMaxBlockSize(),
        agg_step.getMaxBlockSizeForAggregationInOrder(),
        agg_step.getMergeThreads(),
        agg_step.getTemporaryDataMergeThreads(),
        /*storage_has_evenly_distributed_read_=*/false,
        agg_step.isGroupByUseNulls(),
        SortDescription{},
        SortDescription{},
        agg_step.shouldProduceResultsInBucketOrder(),
        agg_step.usingMemoryBoundMerging(),
        /*explicit_sorting_required_for_aggregation_in_order_=*/false);
    merge_step->setStepDescription(fmt::format("Merge: {}", agg_step.getStepDescription()), 200);
    return merge_step;
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

    const auto keys = classifyPushdownKeys(params, pushed_header, other_header, pushed_bindings, other_bindings, pushed_condition_inputs);
    if (!keys)
        return nullptr;

    /// Cardinality gate, shared by variants A and B (for B `pushed_keys` is exactly `G`, see
    /// above): the pushed input group's statistics are already derived by the time this rule
    /// runs (`ApplyRuleTask` derives them recursively through inputs before applying a rule).
    /// Bail without them, or without every pushed key's NDV, or without a proven guarantee that
    /// the widened key set shrinks the input - never let this rewrite be priced by a guess.
    const auto & pushed_input_group = *memo.getGroup(match.join_expression->inputs[pushed_input_index].group_id);
    if (!pushed_input_group.statistics
        || !pushedKeysHaveReliableCardinality(*pushed_input_group.statistics, keys->pushed_keys)
        || !pushedKeysGuaranteeReduction(*pushed_input_group.statistics, keys->pushed_keys))
        return nullptr;

    /// Variant B: every `GROUP BY` key is on the pushed side and the join condition reads only
    /// `GROUP BY` keys of it, so the aggregation stays final below the join and no merge is
    /// needed above. B strictly dominates A here (same grouping, no merge) - emit only B.
    const auto & join_operator = join_step.getJoinOperator();
    const bool full_pushdown = !keys->has_other_side_keys && !keys->condition_extends_keys
        && isFullPushdownAllowed(join_operator.kind, join_operator.strictness, side);

    /// `distributed_plan_force_shuffle_aggregation` forbids the partial + merge split that
    /// variant A is; variant B keeps a single final aggregation and stays legal under it.
    if (!full_pushdown && memo.getContext().distributed_plan_force_shuffle_aggregation)
        return nullptr;

    /// The rebuilt pushed-side header (keys + aggregate state columns) must have unique names
    /// disjoint from the other side's - `JoinExpressionActions` requires it.
    NameSet new_pushed_names = keys->pushed_key_set;
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
    /// The cloned step still carries the hash-table stats identity stamped for the original
    /// post-join aggregation (`setAggregationHashTableCacheKeys`, pre-Cascades). This step groups
    /// by a different key set over a different input, so a shared key would cross-contaminate
    /// `HashTablesStatistics` size hints between unrelated hash-table shapes; disable it here.
    partial_step->setStatsCacheKey(0);
    if (!full_pushdown)
        partial_step->setFinal(false);
    /// Unlike `TwoStageAggregationTransformation`, no bucket-order forcing under the
    /// memory-efficient merge: bucket annotations do not survive the join anyway, and the merge
    /// treats the join output as single-level data.
    partial_step->rebaseOntoInput(std::move(partial_input_header), keys->pushed_keys);
    partial_step->setStepDescription(
        fmt::format("{}: {}", full_pushdown ? "Pushed" : "Partial", agg_step.getStepDescription()), 200);

    /// The join condition columns must keep their types through the pushed aggregation,
    /// otherwise the rebuilt condition DAG would silently mix types.
    const auto & partial_header = *partial_step->getOutputHeader();
    for (const auto & name : pushed_condition_inputs)
        if (!partial_header.getByName(name).type->equals(*pushed_header.getByName(name).type))
            return nullptr;

    /// The rebuilt join projects exactly the positional layout the merge expects (see
    /// `headerIsKeysThenAggregates`), so this order is load-bearing for variant A; for
    /// variant B (all columns from the pushed side) it equals `params.getHeader` order.
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
        return registerPushdownAlternative(memo, *this, source_expression, match.join_expression, pushed_input_index,
            std::move(pushed_expression), std::move(join_alternative), /*merge_step=*/nullptr);
    }

    auto merge_step = buildMergeStep(agg_step, new_join_step->getOutputHeader());

    if (!check_header(*merge_step->getOutputHeader()))
        return nullptr;

    GroupExpressionPtr partial_expression = std::make_shared<GroupExpression>(std::move(partial_step));
    GroupExpressionPtr join_alternative = std::make_shared<GroupExpression>(std::move(new_join_step));
    return registerPushdownAlternative(memo, *this, source_expression, match.join_expression, pushed_input_index,
        std::move(partial_expression), std::move(join_alternative), std::move(merge_step));
}

std::vector<GroupExpressionPtr> AggregationPushdown::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationPushdown::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());

    std::vector<GroupExpressionPtr> result;
    for (const auto & match : collectJoinsUnderAggregation(*expression, memo, /*stop_at_first_pushable=*/false))
    {
        const auto & join_step = *match.join_step;

        /// `join_use_nulls` type changes and `USING` casts are excluded: an `AggregateFunction`
        /// state column cannot be made `Nullable`.
        if (!join_step.typeChangingSides().empty())
            continue;

        /// When an `ExpressionStep` sits between the aggregation and the join, every needed
        /// column must trace to an identically-named join output; the transformation then just
        /// drops the step. A computed column makes this candidate inapplicable.
        if (match.peeled_expression)
        {
            Names needed_names = agg_step->getParams().keys;
            for (const auto & aggregate : agg_step->getParams().aggregates)
                needed_names.insert(needed_names.end(), aggregate.argument_names.begin(), aggregate.argument_names.end());

            const auto & peeled_dag = match.peeled_expression->getExpression();
            bool traced = true;
            for (const auto & name : needed_names)
            {
                String input_name;
                if (classifyOutputName(peeled_dag, name, input_name) != TranslatedName::Traced || input_name != name)
                {
                    traced = false;
                    break;
                }
            }
            if (!traced)
                continue;
        }

        auto condition_inputs = collectConditionInputs(join_step);
        if (!condition_inputs)
            continue;

        const auto output_bindings = collectOutputBindings(join_step);
        const auto & join_operator = join_step.getJoinOperator();
        for (const auto side : {JoinTableSide::Left, JoinTableSide::Right})
        {
            if (!isPushdownAllowed(join_operator.kind, join_operator.strictness, side))
                continue;
            if (auto alternative = buildPushdownAlternative(expression, match, *condition_inputs, output_bindings, side, memo))
                result.push_back(std::move(alternative));
        }
    }
    return result;
}

}

OptimizationRulePtr createAggregationPushdown() { return std::make_shared<AggregationPushdown>(); }

}
