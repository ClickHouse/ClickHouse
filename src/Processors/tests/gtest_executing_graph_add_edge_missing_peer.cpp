#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <fmt/format.h>

using namespace DB;

/// Regression test for the diagnostics of `ExecutingGraph::addEdge` when a connected peer is
/// missing from the list of processors. The thrown `LOGICAL_ERROR` must identify both endpoints
/// of the broken edge unambiguously, so that repeated processor classes remain distinguishable.
/// Previously the message used `getName`, which is ambiguous, and the numeric suffix was only
/// recoverable from the out-of-set node rendering in `printPipeline` that was removed in #110955.
///
/// The identification must not depend on the execution context: `getUniqID` alone degrades to the
/// `_0` suffix for every processor when `CurrentThread` is not initialized, which is why the
/// message also carries the processor address. This test deliberately does not set up a thread
/// status, so it exercises exactly that degraded context.

namespace
{

/// Must match the format used by `describeProcessor` in `ExecutingGraph.cpp`.
String describeProcessor(const IProcessor & processor)
{
    return fmt::format("{} at {}", processor.getUniqID(), static_cast<const void *>(&processor));
}

struct MalformedGraph
{
    std::shared_ptr<Processors> processors;
    /// The peer is deliberately not in `processors`, but it must outlive the executor
    /// construction: `addEdge` dereferences it to build the exception message.
    ProcessorPtr omitted_sink;
    String source_description;
    String sink_description;
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
    graph.source_description = describeProcessor(*source);
    graph.sink_description = describeProcessor(*sink);
    graph.expected_message = fmt::format(
        "Processor {} was found as output for processor {}, but not found in list of processors",
        graph.sink_description,
        graph.source_description);

    graph.processors = std::make_shared<Processors>();
    graph.processors->emplace_back(std::move(source));
    graph.omitted_sink = std::move(sink);
    return graph;
}

}

#ifdef DEBUG_OR_SANITIZER_BUILD

/// In debug and sanitizer builds a `LOGICAL_ERROR` aborts the process at the point where the
/// exception is constructed, so the message can only be observed through a death test.
TEST(ExecutingGraphDeathTest, AddEdgeMissingPeerIdentifiesBothEndpoints)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    auto graph = makeGraphWithOmittedSink();
    ASSERT_NE(graph.source_description, graph.sink_description);

    /// The `threadsafe` style re-executes the test binary, so the child rebuilds the graph at its
    /// own addresses: match the shape of the message rather than the parent's exact addresses.
    /// That the two endpoints really differ is asserted above, on the in-process descriptions.
    /// Only `.` and `*` are used here: depending on the platform, gtest matches death-test
    /// patterns either with POSIX extended regular expressions or with its own simple engine,
    /// and the latter supports neither character classes nor `\d`.
    const String expected_pattern
        = "Processor NullSink_.* at 0x.* was found as output for processor "
          "SourceFromSingleChunk_.* at 0x.*, but not found in list of processors";

    EXPECT_DEATH(
        {
            QueryStatusPtr element;
            PipelineExecutor executor(graph.processors, element);
        },
        expected_pattern);
}

#else

TEST(ExecutingGraph, AddEdgeMissingPeerIdentifiesBothEndpoints)
{
    auto graph = makeGraphWithOmittedSink();
    ASSERT_NE(graph.source_description, graph.sink_description);

    try
    {
        QueryStatusPtr element;
        PipelineExecutor executor(graph.processors, element);
        FAIL() << "Expected a LOGICAL_ERROR for the processor missing from the list of processors.";
    }
    catch (const DB::Exception & e)
    {
        const auto & message = e.message();
        const auto exception_pos = message.find(graph.expected_message);
        ASSERT_NE(exception_pos, std::string::npos)
            << "Expected '" << graph.expected_message << "', got: " << message;

        /// `PipelineExecutor` appends a `printPipeline` dump to the exception, labelled with the
        /// same `{uniqID} at {address}` format, so the endpoints named by the exception can be
        /// matched against the nodes of the dump even in this degraded no-`CurrentThread`
        /// context. The source is the only processor in the list, so its description must
        /// reappear verbatim in the dump after the exception message.
        ASSERT_NE(message.find("Query pipeline:"), std::string::npos) << "Expected a pipeline dump, got: " << message;
        ASSERT_NE(message.find(graph.source_description, exception_pos + graph.expected_message.size()), std::string::npos)
            << "Expected the pipeline dump to repeat '" << graph.source_description << "', got: " << message;
    }
}

#endif
