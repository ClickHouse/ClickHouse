#include <gtest/gtest.h>

#include <config.h>

#if USE_PARQUET

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/TemporaryFile.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

void writeMultiRowGroupParquet(const String & path, size_t rows_per_group, size_t num_groups)
{
    FormatSettings format_settings;
    format_settings.parquet.row_group_rows = rows_per_group;
    format_settings.parquet.parallel_encoding = false;

    Block header;
    header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "x"));

    Chunks chunks;
    for (size_t group = 0; group < num_groups; ++group)
    {
        auto column = ColumnUInt64::create();
        for (size_t i = 0; i < rows_per_group; ++i)
            column->insert(group * rows_per_group + i);
        chunks.emplace_back(Columns{std::move(column)}, rows_per_group);
    }

    auto source = std::make_shared<SourceFromChunks>(std::make_shared<const Block>(header), std::move(chunks));
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(Pipe(source));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    WriteBufferFromFile write_buffer(path);
    auto output = std::make_shared<ParquetBlockOutputFormat>(write_buffer, pipeline.getSharedHeader(), format_settings, nullptr);
    pipeline.complete(output);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    output->finalize();
    write_buffer.finalize();
}

size_t readNeedOnlyCountTotal(
    ReadBuffer & in,
    const SharedHeader & header,
    const FormatSettings & format_settings,
    FormatParserSharedResourcesPtr parser_shared_resources,
    const FileBucketInfoPtr & buckets)
{
    auto input = std::make_shared<ParquetV3BlockInputFormat>(
        in,
        header,
        format_settings,
        parser_shared_resources,
        std::make_shared<FormatFilterInfo>(),
        /*min_bytes_for_seek=*/ 1024);
    if (buckets)
        input->setBucketsToRead(buckets);
    input->needOnlyCount();

    size_t total = 0;
    while (true)
    {
        Chunk chunk = input->generate();
        if (!chunk)
            break;
        total += chunk.getNumRows();
    }
    return total;
}

std::vector<std::pair<size_t, size_t>> readNeedOnlyCountOffsetsAndRows(
    ReadBuffer & in,
    const SharedHeader & header,
    const FormatSettings & format_settings,
    FormatParserSharedResourcesPtr parser_shared_resources,
    const FileBucketInfoPtr & buckets)
{
    auto input = std::make_shared<ParquetV3BlockInputFormat>(
        in,
        header,
        format_settings,
        parser_shared_resources,
        std::make_shared<FormatFilterInfo>(),
        /*min_bytes_for_seek=*/ 1024);
    if (buckets)
        input->setBucketsToRead(buckets);
    input->needOnlyCount();

    std::vector<std::pair<size_t, size_t>> result;
    while (true)
    {
        Chunk chunk = input->generate();
        if (!chunk)
            break;
        auto info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
        if (!info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ChunkInfoRowNumbers on need_only_count chunk");
        result.emplace_back(info->row_num_offset, chunk.getNumRows());
    }
    return result;
}

}

TEST(ParquetV3NeedOnlyCountBuckets, CountsOnlyAssignedRowGroups)
{
    tryRegisterFormats();
    const auto context = getContext().context;

    Poco::TemporaryFile temp_file;
    const String path = temp_file.path();
    constexpr size_t rows_per_group = 10;
    constexpr size_t num_groups = 3;
    writeMultiRowGroupParquet(path, rows_per_group, num_groups);

    Block header;
    header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "x"));
    auto shared_header = std::make_shared<const Block>(header);
    FormatSettings format_settings;
    auto parser_shared_resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());

    {
        ReadBufferFromFile in(path);
        EXPECT_EQ(
            readNeedOnlyCountTotal(in, shared_header, format_settings, parser_shared_resources, /*buckets=*/ nullptr),
            rows_per_group * num_groups);
    }

    {
        /// Unbucketed need-only-count emits one span per row group (not a single whole-file chunk).
        ReadBufferFromFile in(path);
        const auto offsets_and_rows
            = readNeedOnlyCountOffsetsAndRows(in, shared_header, format_settings, parser_shared_resources, /*buckets=*/ nullptr);
        ASSERT_EQ(offsets_and_rows.size(), num_groups);
        for (size_t group = 0; group < num_groups; ++group)
        {
            EXPECT_EQ(offsets_and_rows[group].first, group * rows_per_group);
            EXPECT_EQ(offsets_and_rows[group].second, rows_per_group);
        }
    }

    {
        ReadBufferFromFile in(path);
        auto buckets = std::make_shared<ParquetFileBucketInfo>(std::vector<size_t>{1});
        EXPECT_EQ(readNeedOnlyCountTotal(in, shared_header, format_settings, parser_shared_resources, buckets), rows_per_group);
    }

    {
        ReadBufferFromFile in(path);
        auto buckets = std::make_shared<ParquetFileBucketInfo>(std::vector<size_t>{0, 2});
        const auto offsets_and_rows
            = readNeedOnlyCountOffsetsAndRows(in, shared_header, format_settings, parser_shared_resources, buckets);
        ASSERT_EQ(offsets_and_rows.size(), 2u);
        EXPECT_EQ(offsets_and_rows[0].first, 0u);
        EXPECT_EQ(offsets_and_rows[0].second, rows_per_group);
        EXPECT_EQ(offsets_and_rows[1].first, 2 * rows_per_group);
        EXPECT_EQ(offsets_and_rows[1].second, rows_per_group);
    }

    {
        ReadBufferFromFile in(path);
        auto buckets = std::make_shared<ParquetFileBucketInfo>(std::vector<size_t>{});
        EXPECT_EQ(readNeedOnlyCountTotal(in, shared_header, format_settings, parser_shared_resources, buckets), 0u);
    }
}

#endif
