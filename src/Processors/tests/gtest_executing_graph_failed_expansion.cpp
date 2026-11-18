#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/V1/ExecutingGraph.h>
#include <Processors/IProcessor.h>
#include <Processors/Sinks/NullSink.h>

using namespace DB;
using namespace DB::Runtime::V1;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

/// A processor that expands the pipeline connects its ports to the new processors before the graph
/// records them, and recording them allocates, so it may fail on the memory limit halfway. The graph
/// is then inconsistent with the ports: a live node is connected to a processor that is not in the
/// graph. The exception cancels the query, but a thread that is already expanding another node walks
/// every node in `addEdges` and used to trip over that connection with a logical error
/// (`Processor ... was found as input for processor ..., but not found in list of processors`), which
/// aborts the server in debug and sanitizer builds. After a failed expansion the graph must refuse
/// further expansions instead.
///
/// The failure is injected through a failpoint, and the graph is driven directly, because the timing
/// window between the two expansions cannot be hit reliably through the executor.

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

/// One output, not connected at first. On its first `prepare` it asks for an expansion, connects the
/// output to a new sink and hands the sink to the graph; afterwards it is finished. With `throw_after_connect`
/// it fails on the memory limit after connecting, before `updatePipeline` returns.
class Expander final : public IProcessor
{
public:
    explicit Expander(SharedHeader header, bool throw_after_connect_ = false)
        : IProcessor({}, OutputPorts(1, header)), throw_after_connect(throw_after_connect_)
    {
    }

    String getName() const override { return "Expander"; }

    Status prepare() override
    {
        return expanded ? Status::Finished : Status::UpdatePipeline;
    }

    void updatePipeline(PipelineUpdate & update) override
    {
        expanded = true;
        auto sink = std::make_shared<NullSink>(outputs.front().getSharedHeader());
        update.to_add.push_back(sink);
        connect(outputs.front(), sink->getPort());
        if (throw_after_connect)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Allocation failure after connecting");
    }

    const bool throw_after_connect;
    bool expanded = false;
};

}

TEST(ExecutingGraphFailedExpansion, LaterExpansionBailsOutInsteadOfLogicalError)
{
    auto header = makeHeader();
    auto first = std::make_shared<Expander>(header);
    auto second = std::make_shared<Expander>(header);

    auto processors = std::make_shared<Processors>();
    processors->push_back(first);
    processors->push_back(second);

    ExecutingGraph graph(processors, /*profile_processors_=*/ false, /*memory_reservation_=*/ nullptr);
    ExecutingGraph::Queue queue;
    ExecutingGraph::Queue async_queue;
    ExecutingGraph::UpdateNodeBuffers buffers;

    /// The initialization prepares both childless expanders and expands the first of them; recording
    /// its new sink fails. The expander's output is connected to that sink by then.
    FailPointInjection::enableFailPoint("executing_graph_add_node_fail");
    try
    {
        graph.initializeExecution(queue, async_queue);
        FailPointInjection::disableFailPoint("executing_graph_add_node_fail");
        FAIL() << "the injected failure did not propagate";
    }
    catch (const Exception & e)
    {
        FailPointInjection::disableFailPoint("executing_graph_add_node_fail");
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    /// Exactly one of them got as far as connecting its output.
    ASSERT_NE(first->expanded, second->expanded);
    auto & not_expanded = first->expanded ? *second : *first;

    /// The other expansion must not walk the inconsistent graph; the query is being cancelled anyway.
    EXPECT_EQ(graph.updateNode(not_expanded, queue, async_queue, buffers), ExecutingGraph::UpdateNodeStatus::Cancelled);
    EXPECT_TRUE(queue.empty());
}

/// The same, but the processor itself fails after connecting its output to the new sink. The sink must stay
/// alive, because the output of the processor points at it, and the graph must refuse further expansions.
TEST(ExecutingGraphFailedExpansion, ProcessorThrowsAfterConnecting)
{
    auto header = makeHeader();
    auto first = std::make_shared<Expander>(header, /*throw_after_connect_=*/ true);
    auto second = std::make_shared<Expander>(header, /*throw_after_connect_=*/ true);

    auto processors = std::make_shared<Processors>();
    processors->push_back(first);
    processors->push_back(second);

    ExecutingGraph graph(processors, /*profile_processors_=*/ false, /*memory_reservation_=*/ nullptr);
    ExecutingGraph::Queue queue;
    ExecutingGraph::Queue async_queue;
    ExecutingGraph::UpdateNodeBuffers buffers;

    try
    {
        graph.initializeExecution(queue, async_queue);
        FAIL() << "the failure in updatePipeline did not propagate";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    ASSERT_NE(first->expanded, second->expanded);
    auto & expanded = first->expanded ? *first : *second;
    auto & not_expanded = first->expanded ? *second : *first;

    /// The sink is kept with the pipeline's processors; reading it through the port is not a use after free.
    EXPECT_EQ(processors->size(), 3);
    EXPECT_EQ(expanded.getOutputs().front().getInputPort().getProcessor().getName(), "NullSink");

    EXPECT_EQ(graph.updateNode(not_expanded, queue, async_queue, buffers), ExecutingGraph::UpdateNodeStatus::Cancelled);
    EXPECT_TRUE(queue.empty());
}
