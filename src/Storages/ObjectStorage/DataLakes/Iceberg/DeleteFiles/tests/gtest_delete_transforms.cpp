#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/DeleteFiles/PositionalDeleteTransform.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ISource.h>
#include <Processors/Sources/SourceFromChunks.h>

#include <Disks/tests/gtest_disk.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromFile.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "Processors/Executors/StreamingFormatExecutor.h"
#include "Processors/Formats/IInputFormat.h"


using namespace DB;

namespace
{

class ChunksSource : public IInputFormat
{
public:
    ChunksSource(Block header_, Chunks chunks_)
        : IInputFormat(header_, nullptr), chunks(std::move(chunks_)), it(chunks.begin()), header(header_)
    {
    }

    String getName() const override { return "ChunksSource"; }

    Chunk read() override
    {
        if (it != chunks.end())
        {
            Chunk && chunk = std::move(*it);
            it++;
            return chunk;
        }
        return {};
    }

private:
    Chunks chunks;
    Chunks::iterator it;
    Block header;
};

std::shared_ptr<ChunksSource> makePositionalDeleteSource(const std::vector<std::vector<std::pair<std::string, int>>> & values)
{
    Block header;
    auto filename_column_type = std::make_shared<DataTypeString>();
    auto positions_column_type = std::make_shared<DataTypeInt64>();
    header.insert(ColumnWithTypeAndName(filename_column_type, PositionalDeleteTransform::filename_column_name));
    header.insert(ColumnWithTypeAndName(positions_column_type, PositionalDeleteTransform::positions_column_name));

    Chunks chunks;
    for (const auto & batch : values)
    {
        Chunk chunk;
        Columns columns;

        auto filename_column = filename_column_type->createColumn();
        auto positions_column = positions_column_type->createColumn();
        for (const auto & value : batch)
        {
            filename_column->insert(value.first);
            positions_column->insert(value.second);
        }
        columns.push_back(std::move(filename_column));
        columns.push_back(std::move(positions_column));
        chunk = Chunk(Columns{std::move(columns)}, values[0].size());
        chunks.push_back(std::move(chunk));
    }

    return std::make_shared<ChunksSource>(header, std::move(chunks));
}


std::shared_ptr<ChunksSource> makeDataSource(const std::vector<std::vector<std::pair<int, int>>> & values)
{
    Block header;
    auto key_column_type = std::make_shared<DataTypeInt64>();
    auto value_column_type = std::make_shared<DataTypeInt64>();
    header.insert(ColumnWithTypeAndName(key_column_type, "key"));
    header.insert(ColumnWithTypeAndName(value_column_type, "value"));

    Chunks chunks;
    size_t total_rows = 0;
    for (const auto & batch : values)
    {
        Chunk chunk;
        Columns columns;

        auto key_column = key_column_type->createColumn();
        auto value_column = key_column_type->createColumn();

        for (auto value : batch)
        {
            key_column->insert(value.first);
            value_column->insert(value.second);
        }
        columns.push_back(std::move(key_column));
        columns.push_back(std::move(value_column));
        chunk = Chunk(Columns{std::move(columns)}, values[0].size());
        chunk.getChunkInfos().add(std::make_shared<ChunkInfoReadRowsBefore>(total_rows));
        total_rows += values[0].size();
        chunks.push_back(std::move(chunk));
    }

    return std::make_shared<ChunksSource>(header, std::move(chunks));
}

}

class IcebergDeletesTest : public ::testing::Test
{
};

TEST(IcebergDeletesTest, PositionalDeleteSimple)
{
    tryRegisterFormats();
    auto delete_file = makePositionalDeleteSource({{{"a", 0}, {"a", 1}}});

    auto source = makeDataSource({{{1, -1}, {2, -1}, {3, -1}}, {{4, -1}, {5, -1}, {6, -1}}});

    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(Pipe(source));
    pipeline_builder.addSimpleTransform(
        [delete_file](const Block & block) -> SimpleTransformPtr
        { return std::make_shared<PositionalDeleteTransform>(block, std::vector<std::shared_ptr<IInputFormat>>{delete_file}, "a"); });
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    WriteBufferFromOwnString out_buf;
    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeInt64>();
        col.name = "key";
        sample.insert(std::move(col));
    }
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeInt64>();
        col.name = "value";
        sample.insert(std::move(col));
    }

    const auto & context_holder = getContext();

    auto output = FormatFactory::instance().getOutputFormat("Values", out_buf, sample, context_holder.context);

    pipeline.complete(output);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    out_buf.finalize();
    ASSERT_EQ(out_buf.str(), "(3,-1),(4,-1),(5,-1),(6,-1)");
}

TEST(IcebergDeletesTest, ChunkCached)
{
    tryRegisterFormats();

    auto delete_file = makePositionalDeleteSource({{{"a", 1}, {"a", 4}, {"b", 0}}});

    auto source = makeDataSource({{{1, -1}, {2, -1}, {3, -1}}, {{4, -1}, {5, -1}, {6, -1}}});

    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(Pipe(source));
    pipeline_builder.addSimpleTransform(
        [delete_file](const Block & block) -> SimpleTransformPtr
        { return std::make_shared<PositionalDeleteTransform>(block, std::vector<std::shared_ptr<IInputFormat>>{delete_file}, "a"); });
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    WriteBufferFromOwnString out_buf;
    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeInt64>();
        col.name = "key";
        sample.insert(std::move(col));
    }
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeInt64>();
        col.name = "value";
        sample.insert(std::move(col));
    }

    const auto & context_holder = getContext();

    auto output = FormatFactory::instance().getOutputFormat("Values", out_buf, sample, context_holder.context);

    pipeline.complete(output);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    out_buf.finalize();
    ASSERT_EQ(out_buf.str(), "(1,-1),(3,-1),(4,-1),(6,-1)");
}
