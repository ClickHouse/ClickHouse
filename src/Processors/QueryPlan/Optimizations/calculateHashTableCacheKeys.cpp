#include <unordered_map>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Joins.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage.h>
#include <Common/SipHash.h>

using namespace DB;

namespace
{

UInt64 calculateHashFromStep(const ReadFromParallelRemoteReplicasStep & source)
{
    SipHash hash;
    hash.update(source.getSerializationName());
    hash.update(source.getStorageID().getFullTableName());
    return hash.get64();
}

UInt64 calculateHashFromStep(const SourceStepWithFilter & read)
{
    SipHash hash;
    hash.update(read.getSerializationName());
    String table_name;
    if (const auto & snapshot = read.getStorageSnapshot())
    {
        StorageID storage_id = snapshot->storage.getStorageID();
        if (storage_id.hasUUID())
            hash.update(storage_id.uuid.toUnderType());
        else
            hash.update(storage_id.getFullTableName());
        table_name = storage_id.getFullTableName();
    }
    if (const auto & dag = read.getPrewhereInfo())
        dag->prewhere_actions.updateHash(hash, /*skip_volatile_constants=*/true);
    return hash.get64();
}

UInt64 calculateHashFromStep(const ITransformingStep & transform)
{
    // The purpose of `HashTablesStatistics` is to provide cardinality estimations.
    // Steps that preserve the number of input rows do not affect cardinality, so we can skip them.
    if (!transform.getTransformTraits().preserves_number_of_rows)
    {
        WriteBufferFromOwnString wbuf;
        SerializedSetsRegistry registry;
        registry.skip_cache_key = true;
        IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry, .skip_final_flag = true, .skip_cache_key = true};

        writeStringBinary(transform.getSerializationName(), wbuf);
        if (transform.isSerializable())
            transform.serialize(ctx);

        SipHash hash;
        hash.update(wbuf.str());
        return hash.get64();
    }
    return 0;
}

}

namespace DB
{

namespace QueryPlanOptimizations
{

UInt64 calculateJoinStepCacheKeyContribution(const JoinStepLogical & join_step, JoinTableSide side)
{
    SipHash hash;

    hash.update(join_step.getSerializationName());
    for (const auto & condition : join_step.getJoinOperator().expression)
    {
        auto [op, lhs, rhs] = condition.asBinaryPredicate();
        if (op == JoinConditionOperator::Equals || op == JoinConditionOperator::NullSafeEquals)
        {
            if (side == JoinTableSide::Left && lhs.fromLeft())
                lhs.getNode()->updateHash(hash, /*skip_volatile_constants=*/true);
            if (side == JoinTableSide::Left && rhs.fromLeft())
                rhs.getNode()->updateHash(hash, /*skip_volatile_constants=*/true);
            if (side == JoinTableSide::Right && lhs.fromRight())
                lhs.getNode()->updateHash(hash, /*skip_volatile_constants=*/true);
            if (side == JoinTableSide::Right && rhs.fromRight())
                rhs.getNode()->updateHash(hash, /*skip_volatile_constants=*/true);
        }
    }

    return hash.get64();
}

void calculateHashTableCacheKeys(
    const QueryPlan::Node & root,
    std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys,
    std::unordered_map<const QueryPlan::Node *, UInt64> & raw_hashes)
{
    struct Frame
    {
        const QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
        // Hash state which steps should update with their own hashes
        SipHash hash{};
    };

    // We use addresses of `left` and `right`, so they should be stable
    std::list<Frame> stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        const auto & node = *frame.node;

        if (auto * join_step = dynamic_cast<JoinStepLogical *>(node.step.get()))
        {
            // `HashTablesStatistics` is used currently only for `parallel_hash_join`, i.e. the following calculation doesn't make sense for other join algorithms.
            const auto & join_expression = join_step->getJoinOperator().expression;
            bool single_disjunct = join_expression.size() > 1 || (join_expression.size() == 1 && !join_expression.front().isFunction(JoinConditionOperator::Or));
            const bool calculate = allowParallelHashJoin(
                join_step->getJoinSettings().join_algorithms,
                join_step->getJoinOperator().kind,
                typeid_cast<JoinStepLogicalLookup *>(node.children.back()->step.get()),
                single_disjunct);

            chassert(node.children.size() == 2);

            if (calculate)
            {
                if (frame.next_child == 0)
                {
                    frame.next_child = node.children.size();
                    stack.push_back({.node = node.children.at(0)});
                    stack.push_back({.node = node.children.at(1)});
                }
                else
                {
                    /// At this point cache_keys[child_i] holds the child's raw bottom-up hash
                    /// (set when the child's frame was popped). Apply this join's per-side
                    /// contribution to produce the child's final cache key, then SipHash-combine
                    /// the two final-keyed children to derive this join's own raw hash.
                    cache_keys[node.children.at(0)] ^= calculateJoinStepCacheKeyContribution(*join_step, JoinTableSide::Left);
                    cache_keys[node.children.at(1)] ^= calculateJoinStepCacheKeyContribution(*join_step, JoinTableSide::Right);
                    frame.hash.update(cache_keys[node.children.at(0)]);
                    frame.hash.update(cache_keys[node.children.at(1)]);
                    const auto raw = frame.hash.get64();
                    raw_hashes[&node] = raw;
                    cache_keys[&node] = raw;

                    stack.pop_back();
                }

                continue;
            }
        }

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child], .hash = frame.hash};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        /// Hash a `JoinStep`'s children in their physical order. `considerEnablingParallelReplicas`
        /// picks the parallelized side by physical slot (child 0, or child 1 for `RIGHT`) — see
        /// `ParallelReplicasLocalPlan::findReadingStep` — and later transplants the single-replica
        /// reading-step analysis onto the parallel-replicas reading step found the same way. So a
        /// hash match must imply both plans put the *same table* in the same physical slot. We must
        /// therefore NOT canonicalize commutative kinds by sorting children: if the two plan builds
        /// pick opposite child orders the parallelized side genuinely differs, and the right outcome
        /// is to NOT match (skip the optimization) rather than transplant cross-table parts/ranges.
        ///
        /// `RIGHT` is the one safe remap: by the equivalence `A RIGHT JOIN B ≡ B LEFT JOIN A` we
        /// swap the children and remap the kind to `LEFT`. This is consistent with the physical
        /// selector, which is also kind-aware (`RIGHT`→child 1, `LEFT`→child 0), so both equivalent
        /// representations resolve to the same table. The (remapped) kind is mixed into the hash so
        /// that otherwise identical subtrees with different kinds (`INNER` vs `LEFT`) do not collide.
        if (const auto * join_step = dynamic_cast<const JoinStep *>(node.step.get()); join_step && node.children.size() == 2)
        {
            const auto & table_join = join_step->getJoin()->getTableJoin();
            auto kind = table_join.kind();

            /// Fold each physical side's equi-keys (with null-safety) and its on-clause residual
            /// condition into that side's child hash, so they travel with the child under the
            /// RIGHT->LEFT remap below (keeping `A RIGHT JOIN B ≡ B LEFT JOIN A`). Two joins over the
            /// same inputs but with different keys/conditions then yield different child
            /// contributions, hence different cache keys, instead of colliding.
            SipHash keys_left;
            SipHash keys_right;
            for (const auto & clause : table_join.getClauses())
            {
                for (size_t i = 0; i < clause.keysCount(); ++i)
                {
                    const bool nullsafe = clause.nullsafe_compare_key_indexes.contains(i);
                    keys_left.update(clause.key_names_left[i]);
                    keys_left.update(nullsafe);
                    keys_right.update(clause.key_names_right[i]);
                    keys_right.update(nullsafe);
                }
                const auto [left_cond, right_cond] = clause.condColumnNames();
                keys_left.update(left_cond);
                keys_right.update(right_cond);
            }
            auto a = cache_keys[node.children.at(0)] ^ keys_left.get64();
            auto b = cache_keys[node.children.at(1)] ^ keys_right.get64();
            if (isRight(kind))
            {
                std::swap(a, b);
                kind = JoinKind::Left;
            }
            /// Orientation-invariant semantics that still change the join's output and therefore the
            /// collected statistics: the (remapped) kind, strictness (ALL/ANY/SEMI/ANTI/ASOF),
            /// `join_use_nulls`, and any residual cross-side predicate in the mixed join expression.
            /// (Locality is not mixed in: it is a distributed-execution strategy and does not change
            /// the join result's cardinality, so it doesn't affect the collected output bytes.)
            frame.hash.update(static_cast<uint8_t>(kind));
            frame.hash.update(static_cast<uint8_t>(table_join.strictness()));
            /// For ASOF the inequality (`<`, `<=`, `>`, `>=`) is part of the join semantics: it
            /// changes which rows match and thus the output, so the same inputs under different
            /// inequalities must not share collected statistics.
            if (table_join.strictness() == JoinStrictness::Asof)
                frame.hash.update(static_cast<uint8_t>(table_join.getAsofInequality()));
            frame.hash.update(table_join.joinUseNulls());
            if (const auto & mixed = table_join.getMixedJoinExpression())
                mixed->getActionsDAG().updateHash(frame.hash, /*skip_volatile_constants=*/true);
            frame.hash.update(a);
            frame.hash.update(b);
        }
        else
        {
            for (const auto * child : node.children)
                frame.hash.update(cache_keys[child]);
        }

        if (const auto * source = dynamic_cast<const ReadFromParallelRemoteReplicasStep *>(node.step.get()))
            frame.hash.update(calculateHashFromStep(*source));
        else if (const auto * read = dynamic_cast<const SourceStepWithFilter *>(node.step.get()))
            frame.hash.update(calculateHashFromStep(*read));
        else if (const auto * transform = dynamic_cast<const ITransformingStep *>(node.step.get()))
            // Completely ignore the ignored steps (i.e. the ones for which we return 0)
            if (auto hash = calculateHashFromStep(*transform))
                frame.hash.update(hash);

        const auto raw = frame.hash.get64();
        raw_hashes[&node] = raw;
        cache_keys[&node] = raw;

        stack.pop_back();
    }
}

static void calculateHashTableCacheKeys(const QueryPlan::Node & root, std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys)
{
    std::unordered_map<const QueryPlan::Node *, UInt64> raw_hashes;
    calculateHashTableCacheKeys(root, cache_keys, raw_hashes);
}

std::unordered_map<const QueryPlan::Node *, UInt64> calculateHashTableCacheKeys(const QueryPlan::Node & root)
{
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    calculateHashTableCacheKeys(root, cache_keys);
    return cache_keys;
}

}
}
