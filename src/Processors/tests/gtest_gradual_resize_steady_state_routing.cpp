#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ResizeProcessor.h>

using namespace DB;

/// `GradualResizeProcessor` funnels the data into a single output while the pre-aggregation stage
/// is still below the row/byte threshold, and routes it to every output once the threshold has been
/// crossed. In that steady state it must pair input `i` with output `i`, the way the pipeline is
/// wired when gradual resize is disabled: many-to-many routing scatters every `GROUP BY` key over
/// all `AggregatingTransform`s, which makes the final merge of heavy aggregate states much more
/// expensive for data that is localized by the grouping key.
///
/// The routing is not observable from SQL - the query answer is the same either way - hence a
/// processor-level test.

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

Chunk makeChunk(UInt64 value)
{
    Columns columns;
    columns.emplace_back(ColumnUInt64::create(1, value));
    return Chunk(std::move(columns), 1);
}

UInt64 valueOf(const Chunk & chunk)
{
    return chunk.getColumns().at(0)->getUInt(0);
}

/// A processor is only needed as an owner of the ports the resize processor is connected to;
/// its own `prepare` is never called, the ports are driven by the test directly.
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

/// Two inputs and two outputs, with a row threshold of one chunk, so that the first routed chunk
/// already activates all outputs and everything that follows is routed in the steady state.
struct Fixture
{
    SharedHeader header = makeHeader();
    PortOwner upstream{header, 0, 2};
    PortOwner downstream{header, 2, 0};
    GradualResizeProcessor resize{header, 2, 2, /* min_rows_per_output = */ 1, /* min_bytes_per_output = */ 0};

    InputPort & in0 = *resize.getInputs().begin();
    InputPort & in1 = *std::next(resize.getInputs().begin());
    OutputPort & out0 = *resize.getOutputs().begin();
    OutputPort & out1 = *std::next(resize.getOutputs().begin());

    OutputPort & upstream_out0 = *upstream.getOutputs().begin();
    OutputPort & upstream_out1 = *std::next(upstream.getOutputs().begin());
    InputPort & downstream_in0 = *downstream.getInputs().begin();
    InputPort & downstream_in1 = *std::next(downstream.getInputs().begin());

    Fixture()
    {
        connect(upstream_out0, in0);
        connect(upstream_out1, in1);
        connect(out0, downstream_in0);
        connect(out1, downstream_in1);
    }

    /// Drives the ramp-up: both consumers ask for data, but only the first output is active, so the
    /// chunk of the second input is routed to the first output. That chunk crosses the threshold.
    void rampUp()
    {
        downstream_in0.setNeeded();
        downstream_in1.setNeeded();
        ASSERT_EQ(resize.prepare({}, {&out0, &out1}), IProcessor::Status::NeedData);

        upstream_out1.push(makeChunk(1));
        resize.prepare({&in1}, {});

        ASSERT_TRUE(downstream_in0.hasData());
        ASSERT_FALSE(downstream_in1.hasData());
    }
};

}

TEST(GradualResizeProcessor, SteadyStateRoutingIsOneToOne)
{
    Fixture f;
    f.rampUp();

    /// The first output is served and becomes free again.
    ASSERT_EQ(valueOf(f.downstream_in0.pull()), 1u);
    f.resize.prepare({}, {&f.out0});

    /// Both inputs now have data and both outputs need data.
    f.upstream_out0.push(makeChunk(10));
    f.upstream_out1.push(makeChunk(11));
    f.resize.prepare({&f.in0, &f.in1}, {});

    ASSERT_TRUE(f.downstream_in0.hasData());
    ASSERT_TRUE(f.downstream_in1.hasData());

    /// Input 0 goes to output 0 and input 1 goes to output 1. Many-to-many routing would hand the
    /// outputs out in the order they asked for data, which is the opposite pairing here.
    EXPECT_EQ(valueOf(f.downstream_in0.pull()), 10u);
    EXPECT_EQ(valueOf(f.downstream_in1.pull()), 11u);
}

TEST(GradualResizeProcessor, SteadyStateKeepsDataWhenPairedOutputIsBusy)
{
    Fixture f;
    f.rampUp();

    /// The first output still holds the chunk of the ramp-up, so it cannot take another one. The
    /// chunk of input 0 must stay in its input port instead of being handed to the free output 1.
    f.upstream_out0.push(makeChunk(10));
    f.resize.prepare({&f.in0}, {});

    EXPECT_FALSE(f.downstream_in1.hasData());
    EXPECT_TRUE(f.in0.hasData());

    /// Once the paired output is free, the chunk goes there.
    ASSERT_EQ(valueOf(f.downstream_in0.pull()), 1u);
    f.resize.prepare({}, {&f.out0});

    ASSERT_TRUE(f.downstream_in0.hasData());
    EXPECT_EQ(valueOf(f.downstream_in0.pull()), 10u);
    EXPECT_FALSE(f.downstream_in1.hasData());
}

TEST(GradualResizeProcessor, SteadyStateFallsBackWhenPairedOutputIsFinished)
{
    Fixture f;
    f.rampUp();

    /// The consumer of the first output goes away. Its data has to be aggregated somewhere, so the
    /// chunks of input 0 are handed to another output that needs data instead of being stranded.
    f.downstream_in0.close();
    f.resize.prepare({}, {&f.out0});

    f.upstream_out0.push(makeChunk(10));
    f.resize.prepare({&f.in0}, {});

    ASSERT_TRUE(f.downstream_in1.hasData());
    EXPECT_EQ(valueOf(f.downstream_in1.pull()), 10u);

    /// And the processor still terminates when the inputs are done.
    f.upstream_out0.finish();
    f.upstream_out1.finish();
    EXPECT_EQ(f.resize.prepare({&f.in0, &f.in1}, {}), IProcessor::Status::Finished);
}

TEST(GradualResizeProcessor, SteadyStateReusesOutputOfFinishedInput)
{
    Fixture f;
    f.rampUp();

    /// Input 1 runs out of data early. Its output still needs data, and without a rebalance it would
    /// stay idle for the rest of the query while input 0 keeps producing.
    f.upstream_out1.finish();
    f.resize.prepare({&f.in1}, {});

    /// The first output still holds the chunk of the ramp-up, so the chunk of input 0 goes to the
    /// output of the finished input instead of waiting for its own output.
    f.upstream_out0.push(makeChunk(10));
    f.resize.prepare({&f.in0}, {});

    ASSERT_TRUE(f.downstream_in1.hasData());
    EXPECT_EQ(valueOf(f.downstream_in1.pull()), 10u);
    EXPECT_FALSE(f.in0.hasData());

    /// Once its own output is free, input 0 goes there again.
    ASSERT_EQ(valueOf(f.downstream_in0.pull()), 1u);
    f.resize.prepare({}, {&f.out0, &f.out1});

    f.upstream_out0.push(makeChunk(20));
    f.resize.prepare({&f.in0}, {});

    ASSERT_TRUE(f.downstream_in0.hasData());
    EXPECT_EQ(valueOf(f.downstream_in0.pull()), 20u);
    EXPECT_FALSE(f.downstream_in1.hasData());

    f.upstream_out0.finish();
    EXPECT_EQ(f.resize.prepare({&f.in0}, {}), IProcessor::Status::Finished);
}
