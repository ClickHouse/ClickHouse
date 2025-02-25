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
    WriteBufferFromOwnString wbuf;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry};

    writeStringBinary(read.getSerializationName(), wbuf);
    if (const auto & snapshot = read.getStorageSnapshot())
        writeStringBinary(snapshot->storage.getStorageID().getFullTableName(), wbuf);
    if (const auto & dag = read.getPrewhereInfo())
        dag->prewhere_actions.serialize(ctx.out, ctx.registry);

    SipHash hash;
    hash.update(wbuf.str());
    return hash.get64();
}

UInt64 calculateHashFromStep(const ITransformingStep & transform)
{
    if (!transform.getTransformTraits().preserves_number_of_rows)
    {
        WriteBufferFromOwnString wbuf;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry};

        writeStringBinary(transform.getSerializationName(), wbuf);
        try
        {
            transform.serialize(ctx);
        }
        catch (const Exception & e)
        {
            // Some steps currently missing serialization support. Let's just skip them.
            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                throw;
        }

        SipHash hash;
        hash.update(wbuf.str());
        return hash.get64();
    }
    return 0;
}

UInt64 calculateHashFromStep(const JoinStepLogical & join_step, JoinTableSide side)
{
    WriteBufferFromOwnString wbuf;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry};

    writeStringBinary(join_step.getSerializationName(), wbuf);
    const auto & pre_join_actions = side == JoinTableSide::Left ? join_step.getExpressionActions().left_pre_join_actions
                                                                : join_step.getExpressionActions().right_pre_join_actions;
    chassert(pre_join_actions);
    pre_join_actions->serialize(ctx.out, ctx.registry);
    chassert(join_step.getJoinInfo().expression.disjunctive_conditions.empty(), "ConcurrentHashJoin supports only one disjunct currently");
    writeBinary(join_step.getJoinInfo().expression.condition.predicates.size(), wbuf);
    for (const auto & pred : join_step.getJoinInfo().expression.condition.predicates)
    {
        const auto & node = side == JoinTableSide::Left ? pred.left_node : pred.right_node;
        writeStringBinary(node.getColumnName(), wbuf);
        writeBinary(static_cast<UInt8>(pred.op), wbuf);
    }

    SipHash hash;
    hash.update(wbuf.str());
    return hash.get64();
}

}

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

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
        auto & node = *frame.node;

        if (auto * join_step = dynamic_cast<JoinStepLogical *>(node.step.get()))
        {
            // `HashTablesStatistics` is used currently only for `parallel_hash_join`, i.e. the following calculation doesn't make sense for other join algorithms.
            const bool calculate = frame.hash
                || allowParallelHashJoin(
                                       join_step->getJoinSettings().join_algorithm,
                                       join_step->getJoinInfo().kind,
                                       join_step->getJoinInfo().strictness,
                                       join_step->hasPreparedJoinStorage(),
                                       join_step->getJoinInfo().expression.disjunctive_conditions.empty());

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
                    join_step->setHashTableCacheKeys(frame.left.get64(), frame.right.get64());
                    if (frame.hash)
                        frame.hash->update(frame.left.get64() ^ frame.right.get64());

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
                frame.hash->update(calculateHashFromStep(*transform));
        }

        stack.pop_back();
    }
}

}
}
