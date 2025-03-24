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
    if (const auto & snapshot = read.getStorageSnapshot())
        hash.update(snapshot->storage.getStorageID().getFullTableName());
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

UInt64 calculateHashFromStep(const JoinStepLogical & join_step, size_t input_num)
{
    SipHash hash;

    BaseRelsSet input_mask = 1u << input_num;
    const auto & join_operator = join_step.getJoinOperator(input_num);

    // `HashTablesStatistics` is used currently only for `parallel_hash_join`, i.e. the following calculation doesn't make sense for other join algorithms.
    const bool calculate = allowParallelHashJoin(
        join_step.getJoinSettings().join_algorithms,
        join_operator.kind,
        join_operator.strictness,
        join_step.hasPreparedJoinStorage(),
        join_operator.expression.disjunctive_conditions.empty());

    if (!calculate)
        return 0;

    auto serialize_join_condition = [&](const JoinCondition & condition)
    {
        hash.update(condition.predicates.size());
        for (const auto & pred : condition.predicates)
        {
            if (pred.op != PredicateOperator::Equals && pred.op != PredicateOperator::NullSafeEquals)
                continue;
            if (pred.left_node.canBeCalculated(input_mask))
                hash.update(pred.left_node.getColumnName());
            if (pred.right_node.canBeCalculated(input_mask))
                hash.update(pred.right_node.getColumnName());
        }
    };

    hash.update(join_step.getSerializationName());
    for (const auto & [mask, pre_join_actions] : join_operator.expression_actions.actions)
    {
        if (mask != input_mask)
            continue;
        chassert(pre_join_actions);
        pre_join_actions->updateHash(hash);
    }

    serialize_join_condition(join_operator.expression.condition);
    for (const auto & condition : join_operator.expression.disjunctive_conditions)
        serialize_join_condition(condition);

    return hash.get64();
}

}

namespace DB
{

namespace QueryPlanOptimizations
{

void calculateHashTableCacheKeys(QueryPlan::Node & root)
{
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
        // Hash state which steps should update with their own hashes
        SipHash * hash = nullptr;

        // Hash states for children of JoinStepLogical
        std::vector<SipHash> children_hashes{};
    };

    // We use addresses of children, so they should be stable
    std::list<Frame> stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        auto & node = *frame.node;

        if (auto * join_step = dynamic_cast<JoinStepLogical *>(node.step.get()))
        {
            chassert(node.children.size() >= 2);

            if (frame.hash)
            {
                if (frame.next_child == 0)
                {
                    /// Initialize hashes for all children
                    frame.children_hashes.resize(node.children.size());

                    frame.next_child = node.children.size();
                    for (size_t i = 0; i < node.children.size(); ++i)
                    {
                        stack.push_back({
                            .node = node.children.at(i),
                            .hash = &frame.children_hashes[i]
                        });
                    }
                }
                else
                {
                    for (size_t input_num = 0; input_num < node.children.size(); ++input_num)
                    {
                        auto step_hash = calculateHashFromStep(*join_step, input_num);
                        if (step_hash == 0)
                            continue;

                        auto & children_hash = frame.children_hashes.at(input_num);
                        children_hash.update(step_hash);
                        join_step->setHashTableCacheKey(children_hash.get64(), input_num);

                        if (frame.hash)
                            frame.hash->update(children_hash);
                    }

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

}
}
