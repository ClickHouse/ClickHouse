#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/ThreadStatus.h>

#include <fmt/format.h>

using namespace DB;

/// Regression test for the diagnostics of `ExecutingGraph::addEdge` when a connected peer is
/// missing from the list of processors. The thrown `LOGICAL_ERROR` must name both endpoints of
/// the broken edge via `getUniqID` (the class name plus the per-thread processor index), so that
/// repeated processor classes remain distinguishable. Previously the message used `getName`,
/// which is ambiguous, and the numeric suffix was only recoverable from the out-of-set node
/// rendering in `printPipeline` that was removed in #110955.

namespace
{

struct MalformedGraph
{
    std::shared_ptr<Processors> processors;
    /// The peer is deliberately not in `processors`, but it must outlive the executor
    /// construction: `addEdge` dereferences it to build the exception message.
    ProcessorPtr omitted_sink;
    String source_uniq_id;
    String sink_uniq_id;
    String expected_message;
};

MalformedGraph makeGraphWithOmittedSink()
{
    auto col = ColumnUInt8::create(1, static_cast<UInt8>(1));
    Columns columns;
    columns.emplace_back(std::move(col));
    Chunk chunk(std::move(columns), 1);

    SharedHeader header = std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});

    auto source = std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk));
    auto sink = std::make_shared<NullSink>(source->getPort().getSharedHeader());

    connect(source->getPort(), sink->getPort());

    MalformedGraph graph;
    graph.source_uniq_id = source->getUniqID();
    graph.sink_uniq_id = sink->getUniqID();
    graph.expected_message = fmt::format(
        "Processor {} was found as output for processor {}, but not found in list of processors",
        graph.sink_uniq_id,
        graph.source_uniq_id);

    graph.processors = std::make_shared<Processors>();
    graph.processors->emplace_back(std::move(source));
    graph.omitted_sink = std::move(sink);
    return graph;
}

}

#ifdef DEBUG_OR_SANITIZER_BUILD

/// In debug and sanitizer builds a `LOGICAL_ERROR` aborts the process at the point where the
/// exception is constructed, so the message can only be observed through a death test.
TEST(ExecutingGraphDeathTest, AddEdgeMissingPeerNamesUniqIDs)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    /// A live `ThreadStatus` makes `getUniqID` assign distinct per-thread indices;
    /// without it every processor gets the `_0` suffix and the test would prove nothing.
    ThreadStatus thread_status;

    auto graph = makeGraphWithOmittedSink();
    ASSERT_NE(graph.source_uniq_id, graph.sink_uniq_id);

    EXPECT_DEATH(
        {
            QueryStatusPtr element;
            PipelineExecutor executor(graph.processors, element);
        },
        graph.expected_message);
}

#else

TEST(ExecutingGraph, AddEdgeMissingPeerNamesUniqIDs)
{
    /// A live `ThreadStatus` makes `getUniqID` assign distinct per-thread indices;
    /// without it every processor gets the `_0` suffix and the test would prove nothing.
    ThreadStatus thread_status;

    auto graph = makeGraphWithOmittedSink();
    ASSERT_NE(graph.source_uniq_id, graph.sink_uniq_id);

    try
    {
        QueryStatusPtr element;
        PipelineExecutor executor(graph.processors, element);
        FAIL() << "Expected a LOGICAL_ERROR for the processor missing from the list of processors.";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_NE(e.message().find(graph.expected_message), std::string::npos)
            << "Expected '" << graph.expected_message << "', got: " << e.message();
    }
}

#endif
