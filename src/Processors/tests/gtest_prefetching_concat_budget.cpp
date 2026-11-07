#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/PrefetchingConcatProcessor.h>
#include <Processors/Port.h>

#include <limits>

using namespace DB;

/// `PrefetchingConcatProcessor` shares the read-ahead budget `max_buffered_bytes` between the buffers
/// of all inputs. The buffers of the later inputs stay resident until the current input is drained,
/// so all of them must be counted against the budget. If the admission check counted only the buffers
/// of the inputs before the one being admitted (a prefix sum), an input that becomes ready before the
/// nearer ones would see an empty prefix and fill the whole budget, and so would every next one: with
/// the inputs becoming ready from the last to the first, the resident bytes would grow with the number
/// of inputs.
///
/// The order in which the upstream sources become ready cannot be controlled from SQL, and the resident
/// bytes are not observable there, hence a processor-level test.

namespace
{

constexpr size_t num_inputs = 8;
constexpr size_t rows_per_chunk = 8192;
constexpr size_t min_buffered_chunks = 2;

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

/// The value encodes the input and the sequence number of the chunk in it, so that the output order can be checked.
Chunk makeChunk(size_t input, size_t seq)
{
    Columns columns;
    columns.emplace_back(ColumnUInt64::create(rows_per_chunk, input * 1000000 + seq));
    return Chunk(std::move(columns), rows_per_chunk);
}

size_t chunkBytes()
{
    return makeChunk(0, 0).allocatedBytes();
}

/// A processor is only needed as an owner of the ports the tested processor is connected to;
/// its own `prepare` is never called, the ports are driven by the test directly.
class PortOwner final : public IProcessor
{
public:
    PortOwner(SharedHeader header, size_t num_inputs_, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs_, header), OutputPorts(num_outputs, header))
    {
    }

    String getName() const override { return "PortOwner"; }
    Status prepare() override { return Status::Finished; }
};

struct Harness
{
    SharedHeader header = makeHeader();
    PortOwner upstream{header, 0, num_inputs};
    PortOwner downstream{header, 1, 0};
    PrefetchingConcatProcessor concat;

    std::vector<OutputPort *> upstream_outputs;
    InputPort & downstream_input;

    std::vector<size_t> pushed = std::vector<size_t>(num_inputs, 0);
    size_t emitted = 0;
    size_t max_resident_chunks = 0;

    explicit Harness(size_t max_buffered_bytes)
        : concat(header, num_inputs, max_buffered_bytes, min_buffered_chunks)
        , downstream_input(downstream.getInputs().front())
    {
        auto concat_input = concat.getInputs().begin();
        for (auto & output : upstream.getOutputs())
        {
            upstream_outputs.push_back(&output);
            connect(output, *concat_input);
            ++concat_input;
        }
        connect(concat.getOutputPort(), downstream_input);
    }

    /// The chunks pulled by the processor from its inputs and not yet pushed to its output.
    size_t residentChunks() const
    {
        size_t in_ports = 0;
        size_t total_pushed = 0;
        for (size_t i = 0; i < num_inputs; ++i)
        {
            total_pushed += pushed[i];
            if (upstream_outputs[i]->hasData())
                ++in_ports;
        }
        return total_pushed - in_ports - emitted - (downstream_input.hasData() ? 1 : 0);
    }

    IProcessor::Status prepare()
    {
        auto status = concat.prepare();
        max_resident_chunks = std::max(max_resident_chunks, residentChunks());
        return status;
    }

    /// Input `idx` produces chunks while the processor asks for them, up to `limit` chunks in total.
    void produce(size_t idx, size_t limit)
    {
        while (pushed[idx] < limit && upstream_outputs[idx]->canPush())
        {
            upstream_outputs[idx]->push(makeChunk(idx, pushed[idx]));
            ++pushed[idx];
            prepare();
        }
    }
};

/// The bound documented for `PrefetchingConcatProcessor`: the budget, exceeded by at most one chunk,
/// plus the head start of the current input. Without a budget, every input has the head start.
size_t allowedResidentBytes(size_t max_buffered_bytes)
{
    if (max_buffered_bytes == 0)
        return num_inputs * min_buffered_chunks * chunkBytes();
    return max_buffered_bytes + chunkBytes() + min_buffered_chunks * chunkBytes();
}

}

TEST(PrefetchingConcatProcessor, BudgetBoundsAllResidentBuffersWhenLaterInputsArriveFirst)
{
    const size_t max_buffered_bytes = 16 * chunkBytes();
    Harness harness(max_buffered_bytes);

    harness.downstream_input.setNeeded();
    ASSERT_EQ(harness.prepare(), IProcessor::Status::NeedData);

    /// Input 0 is waiting, while the inputs from the last to the first become ready, each producing as
    /// much as the processor admits.
    for (size_t idx = num_inputs - 1; idx >= 1; --idx)
        harness.produce(idx, std::numeric_limits<size_t>::max());

    EXPECT_LE(harness.max_resident_chunks * chunkBytes(), allowedResidentBytes(max_buffered_bytes));
    /// The budget is used for read-ahead.
    EXPECT_GE(harness.max_resident_chunks * chunkBytes(), max_buffered_bytes);
    /// Once the budget is exhausted, the later inputs stop reading ahead.
    for (size_t idx = 1; idx < num_inputs; ++idx)
        EXPECT_FALSE(harness.upstream_outputs[idx]->isNeeded()) << idx;
}

/// Drain everything, checking the bound at every step, the progress and the order of the output.
static void checkDrain(size_t max_buffered_bytes)
{
    SCOPED_TRACE(max_buffered_bytes);
    constexpr size_t chunks_per_input = 10;
    Harness harness(max_buffered_bytes);

    harness.downstream_input.setNeeded();
    harness.prepare();
    for (size_t idx = num_inputs - 1; idx >= 1; --idx)
        harness.produce(idx, chunks_per_input);

    std::vector<UInt64> output;
    size_t idle_iterations = 0;
    for (size_t iteration = 0; iteration < 1000000; ++iteration)
    {
        bool progress = false;
        for (size_t idx = 0; idx < num_inputs; ++idx)
        {
            auto & port = *harness.upstream_outputs[idx];
            if (harness.pushed[idx] < chunks_per_input && port.canPush())
            {
                port.push(makeChunk(idx, harness.pushed[idx]));
                ++harness.pushed[idx];
                progress = true;
            }
            else if (harness.pushed[idx] == chunks_per_input && !port.hasData() && !port.isFinished())
            {
                port.finish();
                progress = true;
            }
        }

        auto status = harness.prepare();

        if (harness.downstream_input.hasData())
        {
            auto chunk = harness.downstream_input.pull();
            ++harness.emitted;
            output.push_back(chunk.getColumns()[0]->getUInt(0));
            harness.downstream_input.setNeeded();
            progress = true;
        }

        if (status == IProcessor::Status::Finished)
            break;

        /// `prepare` may have only requested data from an input, which is pushed on the next iteration.
        idle_iterations = progress ? 0 : idle_iterations + 1;
        ASSERT_LT(idle_iterations, 2) << "No progress, status " << IProcessor::statusToName(status);
    }

    EXPECT_LE(harness.max_resident_chunks * chunkBytes(), allowedResidentBytes(max_buffered_bytes));

    ASSERT_EQ(output.size(), num_inputs * chunks_per_input);
    for (size_t idx = 0; idx < num_inputs; ++idx)
        for (size_t seq = 0; seq < chunks_per_input; ++seq)
            EXPECT_EQ(output[idx * chunks_per_input + seq], idx * 1000000 + seq);
}

TEST(PrefetchingConcatProcessor, ProgressAndOrderWithAnyBudget)
{
    checkDrain(0);
    checkDrain(1);
    checkDrain(4 * chunkBytes());
    checkDrain(1000 * chunkBytes());
}

/// When the current input is drained, the next one becomes current and gets the head start. If its chunk
/// is already waiting in the port, because it arrived while the budget was exhausted by the later inputs,
/// the processor must pull it in the same `prepare` call. Returning `NeedData` would hang the pipeline:
/// the data is already there, so nothing would wake the processor up again.
TEST(PrefetchingConcatProcessor, NextInputWithPendingDataOverBudgetIsPulled)
{
    Harness harness(1);

    harness.downstream_input.setNeeded();
    ASSERT_EQ(harness.prepare(), IProcessor::Status::NeedData);

    /// Input 2 takes the whole budget, so input 1 has no room.
    harness.upstream_outputs[2]->push(makeChunk(2, 0));
    ++harness.pushed[2];
    harness.prepare();
    /// Input 1 is visited before input 2 is pulled, so it loses its room on the next call.
    harness.prepare();
    ASSERT_FALSE(harness.upstream_outputs[1]->isNeeded());

    /// A chunk of input 1 arrives anyway, racing with `setNotNeeded`, and waits in the port.
    harness.upstream_outputs[1]->push(makeChunk(1, 0));
    ++harness.pushed[1];
    harness.prepare();
    ASSERT_TRUE(harness.upstream_outputs[1]->hasData());

    /// Input 0 produces one chunk and finishes.
    harness.upstream_outputs[0]->push(makeChunk(0, 0));
    ++harness.pushed[0];
    ASSERT_EQ(harness.prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(harness.downstream_input.hasData());
    EXPECT_EQ(harness.downstream_input.pull().getColumns()[0]->getUInt(0), 0);
    ++harness.emitted;
    harness.downstream_input.setNeeded();
    harness.upstream_outputs[0]->finish();

    /// Input 1 becomes current and its waiting chunk must be output right away.
    ASSERT_EQ(harness.prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(harness.downstream_input.hasData());
    EXPECT_EQ(harness.downstream_input.pull().getColumns()[0]->getUInt(0), 1000000);
}
