#include <gtest/gtest.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/NullSink.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

TEST(Processors, PortsConnected)
{
    auto col = ColumnUInt8::create(1, 1);
    Columns columns;
    columns.emplace_back(std::move(col));
    Chunk chunk(std::move(columns), 1);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")};

    auto source = std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk));
    auto sink = std::make_shared<NullSink>(source->getPort().getHeader());

    connect(source->getPort(), sink->getPort());

    Processors processors;
    processors.emplace_back(std::move(source));
    processors.emplace_back(std::move(sink));

    PipelineExecutor executor(processors);
    executor.execute(1);
}

TEST(Processors, PortsNotConnected)
{
    auto col = ColumnUInt8::create(1, 1);
    Columns columns;
    columns.emplace_back(std::move(col));
    Chunk chunk(std::move(columns), 1);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")};

    auto source = std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk));
    auto sink = std::make_shared<NullSink>(source->getPort().getHeader());

    /// connect(source->getPort(), sink->getPort());

    Processors processors;
    processors.emplace_back(std::move(source));
    processors.emplace_back(std::move(sink));

    try
    {
        PipelineExecutor executor(processors);
        executor.execute(1);
        ASSERT_TRUE(false) << "Should have thrown.";
    }
    catch (DB::Exception & e)
    {
        std::cout << e.displayText() << std::endl;
        ASSERT_TRUE(e.displayText().find("pipeline") != std::string::npos) << "Expected 'pipeline', got: " << e.displayText();
    }
}
