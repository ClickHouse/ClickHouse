#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/ResizeProcessor.h>

using namespace DB;

/// Which output takes a chunk depends on the order in which the ports change, which SQL cannot reproduce deterministically.

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

/// Owns the ports the tested processor is connected to; the test drives the ports directly.
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

struct OverflowFixture
{
    OverflowFixture()
    {
        connect(source, processor.getInputs().front());
        connect(processor.getOutputs().front(), main);
        connect(processor.getOutputs().back(), overflow);
    }

    SharedHeader header = makeHeader();
    PortOwner upstream{header, 0, 1};
    PortOwner downstream{header, 2, 0};
    OverflowProcessor processor{header};

    OutputPort & source = upstream.getOutputs().front();
    InputPort & main = downstream.getInputs().front();
    InputPort & overflow = downstream.getInputs().back();
};

}

TEST(OverflowProcessor, SecondOutputTakesOnlyWhatTheFirstCannot)
{
    OverflowFixture f;
    f.main.setNeeded();
    f.overflow.setNeeded();

    ASSERT_EQ(f.processor.prepare(), IProcessor::Status::NeedData);
    f.source.push(makeChunk(1));
    f.processor.prepare();
    f.source.push(makeChunk(2));
    f.processor.prepare();
    EXPECT_FALSE(f.source.isNeeded());

    f.source.push(makeChunk(3));
    ASSERT_EQ(f.processor.prepare(), IProcessor::Status::PortFull);
    EXPECT_EQ(valueOf(f.main.pull()), 1);
    EXPECT_EQ(valueOf(f.overflow.pull()), 2);

    f.processor.prepare();
    EXPECT_FALSE(f.overflow.hasData());
    EXPECT_EQ(valueOf(f.main.pull()), 3);
}

TEST(OverflowProcessor, FinishedInputFinishesBothOutputs)
{
    OverflowFixture f;
    f.main.setNeeded();
    f.source.push(makeChunk(1));
    f.source.finish();

    ASSERT_EQ(f.processor.prepare(), IProcessor::Status::Finished);
    EXPECT_EQ(valueOf(f.main.pull()), 1);
    EXPECT_TRUE(f.main.isFinished());
    EXPECT_TRUE(f.overflow.isFinished());
}

TEST(OverflowProcessor, ClosedFirstOutputSendsEverythingToTheSecond)
{
    OverflowFixture f;
    f.main.close();
    f.overflow.setNeeded();
    f.source.push(makeChunk(1));

    f.processor.prepare();
    EXPECT_EQ(valueOf(f.overflow.pull()), 1);

    f.overflow.close();
    ASSERT_EQ(f.processor.prepare(), IProcessor::Status::Finished);
    EXPECT_TRUE(f.source.isFinished());
}

TEST(ConcatProcessor, WithoutReadAheadAsksForAChunkOnlyWhenTheOutputCanTakeIt)
{
    auto header = makeHeader();
    PortOwner upstream(header, 0, 2);
    PortOwner downstream(header, 1, 0);
    ConcatProcessor concat(header, 2, /* read_ahead = */ false);
    auto & first = upstream.getOutputs().front();
    auto & output = downstream.getInputs().front();
    connect(first, concat.getInputs().front());
    connect(upstream.getOutputs().back(), concat.getInputs().back());
    connect(concat.getOutputPort(), output);

    output.setNeeded();
    concat.prepare();
    first.push(makeChunk(1));
    concat.prepare();
    EXPECT_FALSE(first.isNeeded());
    EXPECT_FALSE(upstream.getOutputs().back().isNeeded());

    EXPECT_EQ(valueOf(output.pull()), 1);
    concat.prepare();
    EXPECT_TRUE(first.isNeeded());
}
