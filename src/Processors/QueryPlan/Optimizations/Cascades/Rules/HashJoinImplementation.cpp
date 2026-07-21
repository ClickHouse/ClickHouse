#include <DataTypes/getLeastSupertype.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Produces all applicable hash join implementations in a single rule:
///   - Local join: both inputs gathered to one node (always applicable)
///   - Broadcast join: left partitioned any way, right replicated (only when node_count > 1)
///   - Partitioned (shuffle) join: both inputs shuffled by join keys (only when node_count > 1
///     and the join has equi-join predicates)
///
/// When node_count == 1 all three strategies produce the same plan, so only the local join
/// is emitted to avoid redundant identical alternatives in the memo.
class HashJoinImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "HashJoin"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return false; }

    class StrategyEnumerator;

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// Emits the physical alternatives of one logical join into the memo, one method per strategy.
class HashJoinImplementation::StrategyEnumerator
{
public:
    StrategyEnumerator(
        const HashJoinImplementation & rule_,
        GroupExpressionPtr expression_,
        const ExpressionProperties & required_properties_,
        Memo & memo_,
        std::vector<GroupExpressionPtr> & result_);

    void addLocalJoin();
    void addBroadcastJoins(size_t node_count);
    void addShuffleJoin(size_t node_count);
    void addSingleKeyShuffleJoins(size_t node_count);

    bool hasEquiKeys() const { return !equi_keys.empty(); }
    bool hasMultipleEquiKeys() const { return equi_keys.size() >= 2; }
    bool isBroadcastSafe() const { return left_preserved; }

private:
    /// `hash_type_name` is the least supertype both sides are cast to before hashing when
    /// the raw key types differ (otherwise the sides hash to different buckets); empty when
    /// they agree.
    struct JoinKeyPair
    {
        String left;
        String right;
        String hash_type_name;
        String raw_type_name;
    };

    void extractEquiJoinKeys();

    /// Clones the logical join into a physical alternative with the given strategy and
    /// distributions and offers it to the memo.
    void addAlternative(
        ImplementationStrategyPtr strategy,
        const DistributionDescription & left_dist,
        const DistributionDescription & right_dist,
        const DistributionDescription & output_dist,
        String description_suffix = {});

    const HashJoinImplementation & rule;
    GroupExpressionPtr expression;
    const JoinStepLogical & join_step;
    const ExpressionProperties & required_properties;
    Memo & memo;
    std::vector<GroupExpressionPtr> & result;
    std::vector<JoinKeyPair> equi_keys;
    bool needs_hash_cast = false;
    bool left_preserved = false;
    bool right_preserved = false;
};

bool HashJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr &&
        expression->strategy == nullptr;
}

/// A side is preserved when the join never emits rows with that side null/default-extended:
/// its key columns are real values in every output row. Only a preserved side's key columns can
/// be advertised as the output distribution of a shuffle join.
static bool joinPreservesLeftSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

static bool joinPreservesRightSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

HashJoinImplementation::StrategyEnumerator::StrategyEnumerator(
    const HashJoinImplementation & rule_,
    GroupExpressionPtr expression_,
    const ExpressionProperties & required_properties_,
    Memo & memo_,
    std::vector<GroupExpressionPtr> & result_)
    : rule(rule_)
    , expression(std::move(expression_))
    , join_step(*typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep()))
    , required_properties(required_properties_)
    , memo(memo_)
    , result(result_)
{
    extractEquiJoinKeys();

    /// Which sides never emit unmatched null/default-extended rows: every output row carries a
    /// real key from a preserved side. FULL preserves neither side.
    const auto join_kind = join_step.getJoinOperator().kind;
    left_preserved = joinPreservesLeftSide(join_kind);
    right_preserved = joinPreservesRightSide(join_kind);
}

void HashJoinImplementation::StrategyEnumerator::extractEquiJoinKeys()
{
    /// Keys with no common supertype are skipped.
    if (join_step.getJoinOperator().expression.empty())
        return;

    for (const auto & predicate : join_step.getJoinOperator().expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        else if (!left_node.fromLeft() || !right_node.fromRight())
            continue;

        String hash_type_name;
        DataTypePtr left_type = left_node.getType();
        DataTypePtr right_type = right_node.getType();
        if (!left_type->equals(*right_type))
        {
            DataTypePtr common_type = tryGetLeastSupertype(DataTypes{left_type, right_type});
            if (!common_type)
                continue;
            hash_type_name = common_type->getName();
            needs_hash_cast = true;
        }

        equi_keys.push_back({left_node.getColumnName(), right_node.getColumnName(), std::move(hash_type_name), left_type->getName()});
    }
}

void HashJoinImplementation::StrategyEnumerator::addAlternative(
    ImplementationStrategyPtr strategy,
    const DistributionDescription & left_dist,
    const DistributionDescription & right_dist,
    const DistributionDescription & output_dist,
    String description_suffix)
{
    GroupExpressionPtr alternative = std::make_shared<GroupExpression>(*expression);
    alternative->strategy = std::move(strategy);
    alternative->description_suffix = std::move(description_suffix);
    alternative->inputs[0].required_properties.distribution = left_dist;
    alternative->inputs[1].required_properties.distribution = right_dist;
    alternative->properties.distribution = output_dist;

    rule.addPhysicalToMemo(alternative, required_properties, memo, result);
}

/// Local join - both inputs gathered to a single node.
/// Always applicable; when cluster has only 1 node it is also the only strategy
/// because all distributed strategies produce the same plan on a single-node cluster.
void HashJoinImplementation::StrategyEnumerator::addLocalJoin()
{
    DistributionDescription single_node;     /// node_count=1, not replicated (default)
    addAlternative(std::make_shared<LocalJoinStrategy>(), single_node, single_node, single_node);
}

/// Broadcast join - left input partitioned any way across N nodes, right input replicated
/// to all N nodes.
void HashJoinImplementation::StrategyEnumerator::addBroadcastJoins(size_t node_count)
{
    /// Left input: partitioned across N nodes (any column set is acceptable)
    DistributionDescription left_dist;
    left_dist.node_count = node_count;

    /// Right input: replicated to all N nodes
    DistributionDescription right_dist;
    right_dist.node_count = node_count;
    right_dist.is_replicated = true;

    /// The output distribution carries no columns: the left input is allowed to be
    /// partitioned any way, so no specific partitioning can be promised.
    addAlternative(std::make_shared<BroadcastJoinStrategy>(), left_dist, right_dist, left_dist);

    /// Keyed variant: when every parent-required distribution column maps to an
    /// equi-join key or to a left-side column that survives to the join output,
    /// require the left input partitioned by those columns and advertise them on the
    /// output. Every output row stays on its left row's node, so the partitioning
    /// holds; without this variant the parent's requirement would always cost a
    /// shuffle of the joined rows.
    if (required_properties.distribution.columns.empty()
        || required_properties.distribution.is_replicated
        || required_properties.distribution.node_count != node_count)
        return;

    const auto & left_header = join_step.getInputHeaders().front();
    const auto & right_header = join_step.getInputHeaders().back();
    const auto & output_header = join_step.getOutputHeader();

    /// A left column carries its partitioning to the output when it reaches the
    /// output unchanged and cannot be confused with a right-side column.
    auto is_surviving_left_column = [&](const String & name)
    {
        if (!left_header->has(name) || right_header->has(name) || !output_header->has(name))
            return false;
        return left_header->getByName(name).type->equals(*output_header->getByName(name).type);
    };

    DistributionDescription keyed_left_dist;
    keyed_left_dist.node_count = node_count;
    DistributionDescription keyed_output_dist;
    keyed_output_dist.node_count = node_count;

    for (const auto & required_col_set : required_properties.distribution.columns)
    {
        bool found = false;
        for (const auto & key : equi_keys)
        {
            /// A pinned hash type never satisfies the parent's typeless requirement,
            /// so a cast-needing key cannot help here.
            if (!key.hash_type_name.empty())
                continue;
            if (required_col_set.contains(key.left)
                || (right_preserved && required_col_set.contains(key.right)))
            {
                keyed_left_dist.columns.push_back({key.left});
                NameSet output_key;
                output_key.insert(key.left);
                /// The right name is an equivalent partitioning key only when no
                /// null-extended right values can appear (both sides preserved).
                if (right_preserved)
                    output_key.insert(key.right);
                keyed_output_dist.columns.push_back(std::move(output_key));
                found = true;
                break;
            }
        }
        if (!found)
        {
            for (const auto & required_column : required_col_set)
            {
                if (is_surviving_left_column(required_column))
                {
                    keyed_left_dist.columns.push_back({required_column});
                    keyed_output_dist.columns.push_back({required_column});
                    found = true;
                    break;
                }
            }
        }
        if (!found)
            return;
    }

    addAlternative(std::make_shared<BroadcastJoinStrategy>(), keyed_left_dist, right_dist, keyed_output_dist);
}

/// Partitioned (shuffle) join - both inputs shuffled by join key columns.
/// Only applicable when the join has equi-join predicates.
void HashJoinImplementation::StrategyEnumerator::addShuffleJoin(size_t node_count)
{
    /// When any key needs a cast, the cast type must be pinned for every shuffle key so
    /// that the per-key type lists on both sides stay aligned with the key order.
    auto hash_type_for = [](const JoinKeyPair & key) -> const String &
    {
        return key.hash_type_name.empty() ? key.raw_type_name : key.hash_type_name;
    };

    DistributionDescription left_dist;
    left_dist.node_count = node_count;

    DistributionDescription right_dist;
    right_dist.node_count = node_count;

    DistributionDescription output_dist;
    output_dist.node_count = node_count;

    auto add_key = [&](const JoinKeyPair & key)
    {
        left_dist.columns.push_back({key.left});
        right_dist.columns.push_back({key.right});
        if (needs_hash_cast)
        {
            const String & type_name = hash_type_for(key);
            left_dist.hash_type_names.push_back(type_name);
            right_dist.hash_type_names.push_back(type_name);
        }
        /// Advertise the output partitioning only on the preserved side(s).
        NameSet output_key;
        if (left_preserved)
            output_key.insert(key.left);
        if (right_preserved)
            output_key.insert(key.right);
        if (!output_key.empty())
        {
            output_dist.columns.push_back(std::move(output_key));
            if (needs_hash_cast)
                output_dist.hash_type_names.push_back(hash_type_for(key));
        }
    };
    auto clear_keys = [&]()
    {
        left_dist.columns.clear();
        right_dist.columns.clear();
        output_dist.columns.clear();
        left_dist.hash_type_names.clear();
        right_dist.hash_type_names.clear();
        output_dist.hash_type_names.clear();
    };

    /// If the parent requires specific distribution columns, try to match them to join
    /// keys so the join output directly satisfies the parent's distribution requirement.
    /// Fall back to all equi-join keys if not all required columns can be matched.
    /// Note: with pinned hash types the parent requirement (no types) never matches,
    /// so this colocation shortcut applies only to equal-type keys.
    if (!required_properties.distribution.columns.empty())
    {
        bool all_matched = true;
        for (const auto & required_col_set : required_properties.distribution.columns)
        {
            bool found = false;
            for (const auto & key : equi_keys)
            {
                if (required_col_set.contains(key.left) || required_col_set.contains(key.right))
                {
                    add_key(key);
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                all_matched = false;
                break;
            }
        }

        if (!all_matched)
        {
            /// Required columns cannot all be matched to join keys - use all equi-join keys.
            clear_keys();
            for (const auto & key : equi_keys)
                add_key(key);
        }
    }
    else
    {
        for (const auto & key : equi_keys)
            add_key(key);
    }

    addAlternative(std::make_shared<ShuffleJoinStrategy>(), left_dist, right_dist, output_dist);
}

/// Single-key shuffle alternatives.
/// For joins with 2+ equi-join keys, generate a shuffle alternative for each individual
/// key pair. This lets the cost model pick a single-key shuffle when the input is already
/// distributed by that key, avoiding unnecessary re-shuffles.
/// Correctness: hash join on (A=A', B=B') shuffled by only A/A' is correct because
/// matching pairs where A=A' are co-located; B=B' is checked locally in the hash table.
void HashJoinImplementation::StrategyEnumerator::addSingleKeyShuffleJoins(size_t node_count)
{
    for (const auto & key : equi_keys)
    {
        DistributionDescription left_dist;
        left_dist.node_count = node_count;
        left_dist.columns.push_back({key.left});

        DistributionDescription right_dist;
        right_dist.node_count = node_count;
        right_dist.columns.push_back({key.right});

        DistributionDescription output_dist;
        output_dist.node_count = node_count;
        NameSet output_key;
        if (left_preserved)
            output_key.insert(key.left);
        if (right_preserved)
            output_key.insert(key.right);

        if (!key.hash_type_name.empty())
        {
            left_dist.hash_type_names.push_back(key.hash_type_name);
            right_dist.hash_type_names.push_back(key.hash_type_name);
        }
        if (!output_key.empty())
        {
            output_dist.columns.push_back(std::move(output_key));
            if (!key.hash_type_name.empty())
                output_dist.hash_type_names.push_back(key.hash_type_name);
        }

        addAlternative(std::make_shared<ShuffleJoinStrategy>(), left_dist, right_dist, output_dist,
            fmt::format("(by {})", key.left));
    }
}

std::vector<GroupExpressionPtr> HashJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    chassert(typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep()));
    chassert(expression->inputs.size() == 2);

    const size_t cluster_node_count = memo.getEnvironment().cluster_node_count;
    const auto candidate_node_counts = getCandidateNodeCounts(cluster_node_count);

    std::vector<GroupExpressionPtr> result;
    StrategyEnumerator strategies(*this, expression, required_properties, memo, result);

    strategies.addLocalJoin();

    /// For a single-node cluster all distributed strategies are identical to local join - skip them.
    if (candidate_node_counts.empty())
        return result;

    /// Enumerate distributed strategies at each candidate node count.
    for (size_t candidate_node_count : candidate_node_counts)
    {
        /// Broadcast replicates the right side, so it is only safe when every output row is
        /// driven by the partitioned left side: `RIGHT` and `FULL` emit unmatched right-side rows
        /// on every node, and `PASTE` pairs rows by position. `JoinCommutativity` can turn `RIGHT`
        /// Semi/Anti/Any into `LEFT`, but not `RIGHT ALL` or `FULL`.
        if (strategies.isBroadcastSafe())
            strategies.addBroadcastJoins(candidate_node_count);

        if (strategies.hasEquiKeys())
            strategies.addShuffleJoin(candidate_node_count);

        if (strategies.hasMultipleEquiKeys())
            strategies.addSingleKeyShuffleJoins(candidate_node_count);
    }

    return result;
}

OptimizationRulePtr createHashJoinImplementation();
OptimizationRulePtr createHashJoinImplementation() { return std::make_shared<HashJoinImplementation>(); }

}
