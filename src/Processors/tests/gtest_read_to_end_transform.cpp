#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <Processors/Chunk.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ReadToEndTransform.h>

using namespace DB;

namespace
{

SharedHeader oneColumnHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key")});
}

Chunk makeChunk(UInt64 value)
{
    auto column = ColumnUInt64::create();
    column->insertValue(value);
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), 1);
}

}

/// The consumer stops needing the data while a chunk is already buffered in the input of the transform (back-pressure),
/// and then closes the output. The transform discards that chunk, although its input is not needed at that moment,
/// and keeps reading the input until it is finished, including the last chunk that is buffered in a finished input.
TEST(ReadToEndTransform, DrainsInputAfterBackPressureAndClosedOutput)
{
    auto header = oneColumnHeader();
    auto transform = std::make_shared<ReadToEndTransform>(header, nullptr);
    IProcessor & processor = *transform;

    OutputPort feeder(header);
    InputPort consumer(header);
    connect(feeder, transform->getInputs().front());
    connect(transform->getOutputs().front(), consumer);

    consumer.setNeeded();
    ASSERT_EQ(processor.prepare(), IProcessor::Status::NeedData);
    ASSERT_TRUE(feeder.canPush());
    feeder.push(makeChunk(1));

    /// Back-pressure: the chunk stays in the input, which is not needed anymore.
    consumer.setNotNeeded();
    ASSERT_EQ(processor.prepare(), IProcessor::Status::PortFull);
    ASSERT_FALSE(feeder.canPush());

    /// The output is closed: the buffered chunk is discarded, and the input is read further.
    consumer.close();
    ASSERT_EQ(processor.prepare(), IProcessor::Status::NeedData);
    ASSERT_TRUE(feeder.canPush());
    feeder.push(makeChunk(2));
    ASSERT_EQ(processor.prepare(), IProcessor::Status::NeedData);
    ASSERT_TRUE(feeder.canPush());

    /// The source pushes its last chunk and finishes: the transform finishes after discarding it.
    feeder.push(makeChunk(3));
    feeder.finish();
    ASSERT_EQ(processor.prepare(), IProcessor::Status::Finished);
}
