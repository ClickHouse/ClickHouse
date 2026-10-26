#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ResizeProcessor.h>

using namespace DB;

/// `AffinityResizeProcessor` pairs input i with output i. The transitions below depend on the order in which
/// the ports are updated, which cannot be reproduced deterministically from SQL, hence processor-level tests.

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

Chunk makeChunk(UInt8 value)
{
    Columns columns;
    columns.emplace_back(ColumnUInt8::create(1, value));
    return Chunk(std::move(columns), 1);
}

UInt8 valueOf(const Chunk & chunk)
{
    return assert_cast<const ColumnUInt8 &>(*chunk.getColumns().at(0)).getElement(0);
}

/// Owns the ports the resize processor is connected to; the test drives the ports directly.
class PortOwner final : public IProcessor
{
public:
    PortOwner(SharedHeader header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    {
    }

    String getName() const override { return "PortOwner"; }
    Status prepare() override { return Status::Finished; }
};

/// source[i] -> in[i] -> AffinityResize -> out[i] -> sink[i]
struct Fixture
{
    explicit Fixture(size_t num_streams = 2)
        : upstream(header, 0, num_streams)
        , downstream(header, num_streams, 0)
        , resize(header, num_streams)
    {
        auto source_it = upstream.getOutputs().begin();
        auto in_it = resize.getInputs().begin();
        auto out_it = resize.getOutputs().begin();
        auto sink_it = downstream.getInputs().begin();
        for (size_t i = 0; i < num_streams; ++i, ++source_it, ++in_it, ++out_it, ++sink_it)
        {
            connect(*source_it, *in_it);
            connect(*out_it, *sink_it);
            source.push_back(&*source_it);
            in.push_back(&*in_it);
            out.push_back(&*out_it);
            sink.push_back(&*sink_it);
            sink_it->setNeeded();
        }
    }

    SharedHeader header = makeHeader();
    PortOwner upstream;
    PortOwner downstream;
    AffinityResizeProcessor resize;

    std::vector<OutputPort *> source;
    std::vector<InputPort *> in;
    std::vector<OutputPort *> out;
    std::vector<InputPort *> sink;
};

}

TEST(AffinityResizeProcessor, StreamFeedsOnlyItsOwnOutput)
{
    Fixture f;

    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1]}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[0]->isNeeded());
    EXPECT_TRUE(f.source[1]->isNeeded());

    f.source[1]->push(makeChunk(1));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    EXPECT_FALSE(f.sink[0]->hasData());
    ASSERT_TRUE(f.sink[1]->hasData());

    /// The second output is busy, so its input is not asked for more data; the first one still is.
    EXPECT_FALSE(f.source[1]->isNeeded());
    EXPECT_TRUE(f.source[0]->isNeeded());

    EXPECT_EQ(valueOf(f.sink[1]->pull()), 1);
    ASSERT_EQ(f.resize.prepare({}, {f.out[1]}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[1]->isNeeded());

    f.source[1]->push(makeChunk(2));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    ASSERT_TRUE(f.sink[1]->hasData());

    /// A chunk of the second input waits for the second output, although the first output needs data.
    f.source[1]->push(makeChunk(3));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    EXPECT_FALSE(f.sink[0]->hasData());
    EXPECT_TRUE(f.in[1]->hasData());

    EXPECT_EQ(valueOf(f.sink[1]->pull()), 2);
    ASSERT_EQ(f.resize.prepare({}, {f.out[1]}), IProcessor::Status::NeedData);
    ASSERT_TRUE(f.sink[1]->hasData());
    EXPECT_EQ(valueOf(f.sink[1]->pull()), 3);
}

TEST(AffinityResizeProcessor, OnlyOutputOfFinishedInputTakesOtherInputs)
{
    Fixture f(3);

    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1], f.out[2]}), IProcessor::Status::NeedData);
    f.source[2]->finish();
    ASSERT_EQ(f.resize.prepare({f.in[2]}, {}), IProcessor::Status::NeedData);

    f.source[1]->push(makeChunk(1));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    ASSERT_TRUE(f.sink[1]->hasData());

    /// The second output is busy: the chunk goes to the third output, whose input has finished, not to the first.
    f.source[1]->push(makeChunk(2));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    EXPECT_FALSE(f.sink[0]->hasData());
    ASSERT_TRUE(f.sink[2]->hasData());
    EXPECT_EQ(valueOf(f.sink[2]->pull()), 2);
}

TEST(AffinityResizeProcessor, OutputOfFinishedInputTakesOtherInputs)
{
    Fixture f;

    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1]}), IProcessor::Status::NeedData);

    /// The second output gets a chunk of its own input and keeps it (its consumer is busy).
    f.source[1]->push(makeChunk(1));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    ASSERT_TRUE(f.sink[1]->hasData());
    EXPECT_FALSE(f.source[1]->isNeeded());

    /// The first input finishes: its output now waits for any input, so the second one is asked.
    f.source[0]->finish();
    ASSERT_EQ(f.resize.prepare({f.in[0]}, {}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[1]->isNeeded());

    f.source[1]->push(makeChunk(2));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::PortFull);
    ASSERT_TRUE(f.sink[0]->hasData());
    EXPECT_EQ(valueOf(f.sink[0]->pull()), 2);
    EXPECT_EQ(valueOf(f.sink[1]->pull()), 1);

    /// Both outputs wait now; the own output of the second input is preferred, and the input is still
    /// asked for data because the first output waits.
    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1]}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[1]->isNeeded());
    f.source[1]->push(makeChunk(3));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {}), IProcessor::Status::NeedData);
    ASSERT_TRUE(f.sink[1]->hasData());
    EXPECT_FALSE(f.sink[0]->hasData());
    EXPECT_TRUE(f.source[1]->isNeeded());
    EXPECT_EQ(valueOf(f.sink[1]->pull()), 3);

    f.source[1]->finish();
    EXPECT_EQ(f.resize.prepare({f.in[1]}, {f.out[1]}), IProcessor::Status::Finished);
    EXPECT_TRUE(f.out[0]->isFinished());
    EXPECT_TRUE(f.out[1]->isFinished());
}

TEST(AffinityResizeProcessor, ChunkOfInputWithFinishedOutputGoesToAnyOutput)
{
    Fixture f;

    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1]}), IProcessor::Status::NeedData);

    /// The consumer of the second output stops reading.
    f.sink[1]->close();
    f.source[1]->push(makeChunk(7));
    ASSERT_EQ(f.resize.prepare({f.in[1]}, {f.out[1]}), IProcessor::Status::PortFull);
    ASSERT_TRUE(f.sink[0]->hasData());
    EXPECT_EQ(valueOf(f.sink[0]->pull()), 7);

    /// Once every output is finished, the inputs are closed.
    f.sink[0]->close();
    EXPECT_EQ(f.resize.prepare({}, {f.out[0]}), IProcessor::Status::Finished);
    EXPECT_TRUE(f.in[0]->isFinished());
    EXPECT_TRUE(f.in[1]->isFinished());
}

TEST(AffinityResizeProcessor, InputOfFinishedOutputIsNeededOnlyWhileAnOutputWaits)
{
    Fixture f;

    ASSERT_EQ(f.resize.prepare({}, {f.out[0], f.out[1]}), IProcessor::Status::NeedData);

    /// The second output finishes while the first one waits: its input may feed the first output.
    f.sink[1]->close();
    ASSERT_EQ(f.resize.prepare({}, {f.out[1]}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[1]->isNeeded());

    /// No output waits, so neither input is asked for data.
    f.source[0]->push(makeChunk(1));
    ASSERT_EQ(f.resize.prepare({f.in[0]}, {}), IProcessor::Status::PortFull);
    EXPECT_FALSE(f.source[0]->isNeeded());
    EXPECT_FALSE(f.source[1]->isNeeded());

    EXPECT_EQ(valueOf(f.sink[0]->pull()), 1);
    ASSERT_EQ(f.resize.prepare({}, {f.out[0]}), IProcessor::Status::NeedData);
    EXPECT_TRUE(f.source[0]->isNeeded());
    EXPECT_TRUE(f.source[1]->isNeeded());
}
