#include <gtest/gtest.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

TEST(Processors, TestReadInt64)
{
    auto col1 = ColumnInt64::create();
    auto col2 = ColumnInt64::create();
    auto col3 = ColumnInt64::create();
    int rows = 500000;
    for (int i = 0; i < rows; ++i)
    {
        col1->insertValue(i);
        col2->insertValue(std::rand());
        col3->insertValue(std::rand());
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    Chunk chunk(std::move(columns), rows);

    Block header = {ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "x"),
                    ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y"),
                    ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "z")};

    auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
    WriteBufferFromFile out("/tmp/test.parquet");
    FormatSettings formatSettings;
    auto parquet_output = std::make_shared<ParquetBlockOutputFormat>(out, header, formatSettings);

    QueryPipelineBuilder builder;
    builder.init(Pipe(source));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    pipeline.complete(std::move(parquet_output));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    parquet::ArrowReaderProperties arrow_properties;
    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    arrow_properties.set_use_threads(false);
    arrow_properties.set_batch_size(8192);

    arrow_properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = 10000000;
    cache_options.range_size_limit = 1l << 40; // reading the whole row group at once is fine
    arrow_properties.set_cache_options(cache_options);
    out.close();
    ReadBufferFromFile in("/tmp/test.parquet");
    std::cerr << in.getFileSize() << std::endl;
    std::atomic<int> is_cancelled{0};
    FormatSettings settings;
    settings.parquet.max_block_size = 8192;
    auto arrow_file = asArrowFile(in, settings, is_cancelled, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);


    ParquetReader reader(header.cloneEmpty(), arrow_properties, reader_properties, arrow_file, settings, {0});

    reader.addFilter("x", std::make_shared<Int64RangeFilter>( 1000, 2000));
    int count = 0;
    while (auto block = reader.read())
    {
        if (block.rows() == 0)
            break;
        count += block.rows();
    }
    ASSERT_EQ(count, 1001);

}
