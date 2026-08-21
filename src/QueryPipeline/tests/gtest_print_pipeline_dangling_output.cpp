#include <gtest/gtest.h>

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <QueryPipeline/printPipeline.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>

#include <memory>
#include <vector>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

/// Minimal processor with a single output port. Used as the surviving upstream.
class OneOutputProcessor final : public IProcessor
{
public:
    explicit OneOutputProcessor(SharedHeader header) : IProcessor({}, {Block(*header)}) { }
    String getName() const override { return "OneOutputProcessor"; }
    Status prepare() override { return Status::Finished; }
};

/// Minimal processor with a single input port. Used as the downstream peer.
class OneInputProcessor final : public IProcessor
{
public:
    explicit OneInputProcessor(SharedHeader header) : IProcessor({Block(*header)}, {}) { }
    String getName() const override { return "OneInputProcessor"; }
    Status prepare() override { return Status::Finished; }
};

}

/// Regression test for the pure-virtual abort (STID 2273-2dbb, issue #110834).
///
/// A surviving processor keeps an output port that is still isConnected() after its downstream
/// peer processor has been destroyed: the shared Port::State is ref-counted, so the survivor's
/// side stays connected, and the raw peer pointer is not cleared on destruction. printPipeline
/// must render only the processors it is given and must not dereference a processor reachable
/// solely through such a dangling port. Before the fix, the node dump virtual-dispatched on the
/// freed peer (getUniqID() -> pure-virtual getName(), SIGABRT), and the edge dump read the freed
/// peer processor (heap-use-after-free under ASAN).
TEST(PrintPipeline, DoesNotDereferenceDanglingOutputPeer)
{
    auto header = makeHeader();

    auto upstream = std::make_shared<OneOutputProcessor>(header);
    auto downstream = std::make_shared<OneInputProcessor>(header);

    connect(upstream->getOutputs().front(), downstream->getInputs().front());
    ASSERT_TRUE(upstream->getOutputs().front().isConnected());

    /// Destroy the downstream peer while the survivor's output port stays connected.
    downstream.reset();
    /// The survivor still reports the port as connected, pointing at the freed peer.
    ASSERT_TRUE(upstream->getOutputs().front().isConnected());

    /// Only the survivor is passed in -- exactly what PipelineExecutor::dumpPipeline does when a
    /// downstream processor was removed/destroyed during pipeline teardown.
    std::vector<IProcessor *> processors{upstream.get()};

    WriteBufferFromOwnString out;
    printPipeline(processors, out);
    out.finalize();

    /// The surviving processor is rendered; the dangling peer is neither labelled nor linked.
    EXPECT_NE(out.str().find("OneOutputProcessor"), std::string::npos);
    EXPECT_EQ(out.str().find("->"), std::string::npos);
}

/// A well-formed pipeline (both processors present) still renders the node and the edge between
/// them, so the fix does not regress normal EXPLAIN PIPELINE / dump output.
TEST(PrintPipeline, DrawsEdgeBetweenProcessorsInSet)
{
    auto header = makeHeader();

    auto upstream = std::make_shared<OneOutputProcessor>(header);
    auto downstream = std::make_shared<OneInputProcessor>(header);
    connect(upstream->getOutputs().front(), downstream->getInputs().front());

    std::vector<IProcessor *> processors{upstream.get(), downstream.get()};

    WriteBufferFromOwnString out;
    printPipeline(processors, out);
    out.finalize();

    const auto dot = out.str();
    EXPECT_NE(dot.find("OneOutputProcessor"), std::string::npos);
    EXPECT_NE(dot.find("OneInputProcessor"), std::string::npos);
    /// Exactly one edge n0 -> n1 between the two processors.
    EXPECT_NE(dot.find("->"), std::string::npos);
}
