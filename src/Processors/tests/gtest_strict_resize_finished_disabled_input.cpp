#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ResizeProcessor.h>

using namespace DB;

/// `StrictResizeProcessor` keeps the inputs that are currently not paired with an output in the
/// `disabled_input_ports` queue. An upstream processor is allowed to finish its output port even
/// when the peer input is not needed, so an input can become finished while it is still sitting in
/// that queue, and the entry cannot be removed from the middle of the queue.
///
/// The processor must not hand a freed output to such an already finished input: the output would
/// be stranded forever (nothing will ever produce data for it), and the input would be counted as
/// finished twice, which breaks the `num_finished_inputs == inputs.size()` termination condition.
///
/// The interleaving cannot be reproduced deterministically from SQL, hence a processor-level test.

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

Chunk makeChunk()
{
    Columns columns;
    columns.emplace_back(ColumnUInt8::create(1, static_cast<UInt8>(1)));
    return Chunk(std::move(columns), 1);
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

}

TEST(StrictResizeProcessor, FinishedDisabledInputDoesNotTakeOutput)
{
    auto header = makeHeader();

    /// Two upstream output ports, two downstream input ports.
    PortOwner upstream(header, 0, 2);
    PortOwner downstream(header, 2, 0);
    StrictResizeProcessor resize(header, 2, 2);

    auto & resize_inputs = resize.getInputs();
    auto & resize_outputs = resize.getOutputs();

    auto & in0 = *resize_inputs.begin();
    auto & in1 = *std::next(resize_inputs.begin());
    auto & out0 = *resize_outputs.begin();
    auto & out1 = *std::next(resize_outputs.begin());

    auto & upstream_out0 = *upstream.getOutputs().begin();
    auto & upstream_out1 = *std::next(upstream.getOutputs().begin());
    auto & downstream_in0 = *downstream.getInputs().begin();
    auto & downstream_in1 = *std::next(downstream.getInputs().begin());

    connect(upstream_out0, in0);
    connect(upstream_out1, in1);
    connect(out0, downstream_in0);
    connect(out1, downstream_in1);

    /// Both consumers ask for data, so both inputs get paired with an output.
    downstream_in0.setNeeded();
    downstream_in1.setNeeded();
    ASSERT_EQ(resize.prepare({}, {&out0, &out1}), IProcessor::Status::NeedData);

    /// The first input delivers a chunk and returns to `disabled_input_ports`.
    upstream_out0.push(makeChunk());
    ASSERT_EQ(resize.prepare({&in0}, {}), IProcessor::Status::PortFull);
    ASSERT_TRUE(downstream_in0.hasData());

    /// It finishes while it is disabled, leaving a stale entry in `disabled_input_ports`.
    upstream_out0.finish();
    ASSERT_EQ(resize.prepare({&in0}, {}), IProcessor::Status::PortFull);

    /// The consumer of the first output takes the chunk, so the output becomes free again.
    downstream_in0.pull();
    ASSERT_TRUE(out0.canPush());
    resize.prepare({}, {&out0});

    /// The freed output must not be given to the finished input: there is no other input for it,
    /// so it has to be closed instead.
    EXPECT_TRUE(out0.isFinished());
    EXPECT_FALSE(upstream_out0.isNeeded());

    /// The finished input must not be counted again, otherwise the processor never terminates.
    EXPECT_NE(resize.prepare({&in0}, {}), IProcessor::Status::Finished);

    upstream_out1.finish();
    EXPECT_EQ(resize.prepare({&in1}, {}), IProcessor::Status::Finished);
}
