#include <unordered_map>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Analyzer/TableFunctionNode.h>
#include <Core/Joins.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

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
    if (const auto & snapshot = read.getStorageSnapshot())
    {
        StorageID storage_id = snapshot->storage.getStorageID();
        if (storage_id.hasUUID())
            hash.update(storage_id.uuid.toUnderType());
        else
            hash.update(storage_id.getFullTableName());
    }
    /// A storage created by a table function has no UUID, and its StorageID does not depend on
    /// the arguments: any numbers(N) reads from `_table_function.numbers`. Mix in the table
    /// function subtree (its name and resolved arguments), otherwise e.g. numbers(1) and
    /// numbers(1e6) would share a stats entry.
    if (const auto & table_expression = read.getQueryInfo().table_expression)
    {
        if (table_expression->as<TableFunctionNode>())
            hash.update(table_expression->getTreeHash({.compare_aliases = false}));
    }
    if (const auto & dag = read.getPrewhereInfo())
        dag->prewhere_actions.updateHash(hash);
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
                lhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Left && rhs.fromLeft())
                rhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Right && lhs.fromRight())
                lhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Right && rhs.fromRight())
                rhs.getNode()->updateHash(hash);
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

        for (const auto * child : node.children)
            frame.hash.update(cache_keys[child]);

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

void setAggregationHashTableCacheKeys(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    if (!optimization_settings.collect_hash_table_stats_during_aggregation)
        return;

    /// Collect the AggregatingStep nodes first. If there are none, do nothing — and in particular do
    /// NOT hash the plan: hashing serializes plan steps, and some steps that may appear in arbitrary
    /// queries are not serializable (e.g. `ORDER BY ... WITH FILL`, or `INSERT ... FROM INFILE`
    /// plans) and would throw. Only aggregating queries need a key.
    std::vector<QueryPlan::Node *> aggregating_nodes;
    {
        std::vector<QueryPlan::Node *> stack;
        stack.push_back(&root);
        while (!stack.empty())
        {
            auto * node = stack.back();
            stack.pop_back();
            if (typeid_cast<AggregatingStep *>(node->step.get()))
                aggregating_nodes.push_back(node);
            for (auto * child : node->children)
                stack.push_back(child);
        }
    }

    /// Compute every key before stamping any of them, so a key never depends on whether another
    /// (e.g. a nested) aggregation has already been stamped.
    std::vector<std::pair<AggregatingStep *, UInt64>> keys_to_set;
    for (auto * aggregating_node : aggregating_nodes)
    {
        /// Hash only the aggregation's own input subtree (this node as the root). This excludes the
        /// steps ABOVE the aggregation (LIMIT, `ORDER BY ... WITH FILL`, ...) — they do not affect
        /// the number of groups and may not be serializable.
        try
        {
            const auto cache_keys = calculateHashTableCacheKeys(*aggregating_node);
            if (auto it = cache_keys.find(aggregating_node); it != cache_keys.end())
                keys_to_set.emplace_back(typeid_cast<AggregatingStep *>(aggregating_node->step.get()), it->second);
        }
        catch (...)
        {
            /// The key is a best-effort preallocation hint. If a step inside the input subtree is not
            /// serializable, skip preallocation for this aggregation rather than fail the query.
            LOG_TRACE(
                getLogger("QueryPlanOptimizations"),
                "Skipping aggregation hash-table preallocation key: {}",
                getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }
    }

    for (auto & [aggregating_step, key] : keys_to_set)
        aggregating_step->setStatsCacheKey(key);
}

}
}
