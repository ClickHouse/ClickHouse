#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <fmt/format.h>

using namespace DB;

/// Regression test for the diagnostics of `ExecutingGraph::addEdge` when a connected peer is
/// missing from the list of processors. The thrown `LOGICAL_ERROR` must name both endpoints of the
/// broken edge, and refer to the node id of the processor in the list, which is the id the appended
/// pipeline dump uses. The message must not contain processor addresses.

namespace
{

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
    graph.source_description = source->getUniqID();
    graph.sink_description = sink->getUniqID();
    graph.expected_message = fmt::format(
        "Processor {} is connected to an output port of processor n0 {}, but it is not in the list of processors. "
        "See n0 in the pipeline dump",
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

    EXPECT_DEATH(
        {
            QueryStatusPtr element;
            PipelineExecutor executor(graph.processors, element);
        },
        graph.expected_message);
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

        /// `PipelineExecutor` appends a `printPipeline` dump to the exception. The source is the
        /// only processor in the list, so the dump must label it as node `n0`.
        ASSERT_NE(message.find("Query pipeline:"), std::string::npos) << "Expected a pipeline dump, got: " << message;
        const String dump_node = fmt::format("n0[label=\"{}", graph.source_description);
        ASSERT_NE(message.find(dump_node, exception_pos + graph.expected_message.size()), std::string::npos)
            << "Expected the pipeline dump to contain '" << dump_node << "', got: " << message;
        ASSERT_EQ(message.find("0x"), std::string::npos) << "Expected no addresses, got: " << message;
    }
}

#endif
