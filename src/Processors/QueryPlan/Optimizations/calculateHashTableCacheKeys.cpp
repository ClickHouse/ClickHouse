#include <unordered_map>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Joins.h>
#include <IO/WriteHelpers.h>
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
        IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry};

        writeStringBinary(transform.getSerializationName(), wbuf);
        if (transform.isSerializable())
            transform.serialize(ctx);

        SipHash hash;
        hash.update(wbuf.str());
        return hash.get64();
    }
    return 0;
}

UInt64 calculateHashFromStep(const JoinStepLogical & join_step, JoinTableSide side)
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

}

namespace DB
{

namespace QueryPlanOptimizations
{

void calculateHashTableCacheKeys(const QueryPlan::Node & root, std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys)
{
    struct Frame
    {
        const QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
        // Hash state which steps should update with their own hashes
        SipHash * hash = nullptr;
        // Hash state for left and right children of JoinStepLogical,
        // kept in frame object since we cannot allocate them on the stack
        SipHash left{};
        SipHash right{};
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
            const bool calculate = frame.hash
                || allowParallelHashJoin(
                                       join_step->getJoinSettings().join_algorithms,
                                       join_step->getJoinOperator().kind,
                                       join_step->getJoinOperator().strictness,
                                       typeid_cast<JoinStepLogicalLookup *>(node.children.back()->step.get()),
                                       single_disjunct);

            chassert(node.children.size() == 2);

            if (calculate)
            {
                if (frame.next_child == 0)
                {
                    frame.next_child = node.children.size();
                    stack.push_back({.node = node.children.at(0), .hash = &frame.left});
                    stack.push_back({.node = node.children.at(1), .hash = &frame.right});
                }
                else
                {
                    frame.left.update(calculateHashFromStep(*join_step, JoinTableSide::Left));
                    frame.right.update(calculateHashFromStep(*join_step, JoinTableSide::Right));

                    auto left_val = frame.left.get64();
                    auto right_val = frame.right.get64();

                    cache_keys[node.children.at(0)] = left_val;
                    cache_keys[node.children.at(1)] = right_val;
                    cache_keys[&node] = left_val ^ right_val;

                    if (frame.hash)
                        frame.hash->update(left_val ^ right_val);

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

        if (frame.hash)
        {
            if (const auto * source = dynamic_cast<const ReadFromParallelRemoteReplicasStep *>(node.step.get()))
                frame.hash->update(calculateHashFromStep(*source));
            else if (const auto * read = dynamic_cast<const SourceStepWithFilter *>(node.step.get()))
                frame.hash->update(calculateHashFromStep(*read));
            else if (const auto * transform = dynamic_cast<const ITransformingStep *>(node.step.get()))
                // Completely ignore the ignored steps (i.e. the ones for which we return 0)
                if (auto hash = calculateHashFromStep(*transform))
                    frame.hash->update(hash);
        }

        stack.pop_back();
    }
}

std::unordered_map<const QueryPlan::Node *, UInt64> calculateHashTableCacheKeys(const QueryPlan::Node & root)
{
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    calculateHashTableCacheKeys(root, cache_keys);
    return cache_keys;
}

}
}
