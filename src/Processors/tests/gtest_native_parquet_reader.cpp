#include <gtest/gtest.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Functions/FunctionFactory.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>

using namespace DB;


void headBlock(const DB::Block & block, size_t count)
{
    std::cout << "============Block============" << std::endl;
    std::cout << block.dumpStructure() << std::endl;
    // print header
    for (const auto & name : block.getNames())
        std::cout << name << "\t";
    std::cout << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;

            if (column > 0)
                std::cout << "\t";
            DB::WhichDataType which(type);
            if (which.isAggregateFunction())
                std::cout << "Nan";
            else if (col->isNullAt(row))
                std::cout << "null";
            else
                std::cout << toString((*col)[row]);
        }
        std::cout << std::endl;
    }
}


void writeParquet(Chunk && chunk, const Block & header, const String & path)
{
    WriteBufferFromFile out(path);
    FormatSettings formatSettings;
    formatSettings.parquet.use_native_reader = false;
    formatSettings.parquet.use_custom_encoder = false;
    auto parquet_output = std::make_shared<ParquetBlockOutputFormat>(out, header, formatSettings);

    QueryPipelineBuilder builder;
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(header, std::move(chunk))));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    pipeline.complete(std::move(parquet_output));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

std::unique_ptr<ParquetReader> openParquet(const Block & header, ReadBufferFromFile & in)
{
    parquet::ArrowReaderProperties arrow_properties;
    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    arrow_properties.set_use_threads(false);
    arrow_properties.set_batch_size(DEFAULT_BLOCK_SIZE);

    arrow_properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = 10000000;
    cache_options.range_size_limit = 1l << 40; // reading the whole row group at once is fine
    arrow_properties.set_cache_options(cache_options);
    std::atomic<int> is_cancelled{0};
    FormatSettings settings;
    settings.parquet.max_block_size = 8192;
    auto arrow_file = asArrowFile(in, settings, is_cancelled, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
    return std::make_unique<ParquetReader>(
        header.cloneEmpty(), in, arrow_properties, reader_properties, arrow_file, settings);
}

void testOldParquet(const Block & header, ReadBufferFromFile & in)
{
    QueryPipelineBuilder builder;
    FormatSettings settings;

    auto format = std::make_shared<ParquetBlockInputFormat>(in, header, settings, 1, 1024 * 1024);
    builder.init(Pipe(format));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);
    int count [[maybe_unused]] = 0;
    while (true)
    {
        Block block;
        executor.pull(block);
        count += block.rows();
        if (block.rows() == 0)
            break;
    }
    //    std::cerr << "total count: " << count << std::endl;
}

void benchmark(String name, int count, std::function<void()> testcase)
{
    std::vector<size_t> times;
    for (int i = 0; i < count; ++i)
    {
        Stopwatch time;
        testcase();
        times.push_back(time.elapsedMicroseconds());
        //        std::cerr << "iteration: " << i << " time : " << time.elapsedMilliseconds() << std::endl;
    }
    std::cerr << name << " Time: " << *std::min_element(times.begin(), times.end()) << std::endl;
}

TEST(Processors, BenchmarkReadInt64)
{
    auto col1 = ColumnInt64::create();
    auto col2 = ColumnInt64::create();
    auto col3 = ColumnInt64::create();
    auto col4 = ColumnInt64::create();
    auto col5 = ColumnInt64::create();
    auto col6 = ColumnInt64::create();
    auto col7 = ColumnInt64::create();

    int rows = 10000000;
    for (int i = 0; i < rows; ++i)
    {
        col1->insertValue(std::rand() % 100);
        col2->insertValue(i);
        col3->insertValue(i+1);
        col4->insertValue(i+2);
        col5->insertValue(i+3);
        col6->insertValue(i+4);
        col7->insertValue(i+5);
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    columns.emplace_back(std::move(col4));
    columns.emplace_back(std::move(col5));
    columns.emplace_back(std::move(col6));
    columns.emplace_back(std::move(col7));

    Chunk chunk(std::move(columns), rows);

    Block header
        = {ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "x"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y1"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y2"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y3"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y4"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "y5"),
           ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "z")};
    const auto * path = "/tmp/test.parquet";
    writeParquet(std::move(chunk), header, path);

    auto old_test [[maybe_unused]] = [&]()
    {
        ReadBufferFromFile in(path);
        testOldParquet(header, in);
    };

    auto new_test = [&]()
    {
        ReadBufferFromFile in(path);
        auto reader = openParquet(header, in);
        reader->addFilter("x", std::make_shared<Int64RangeFilter>(-1, -1, false));
        int count [[maybe_unused]] = 0;
        while (auto block = reader->read())
        {
            count += block.rows();
            if (block.rows() == 0)
                break;
        }
        std::cerr << "total count: " << count << std::endl;
    };
    std::cerr << "start benchmark \n";
//    benchmark("arrow", 50, old_test);
    benchmark("native", 1, new_test);
}


TEST(Processors, TestReadNullableInt64)
{
    auto type = makeNullable(std::make_shared<DataTypeInt64>());
    auto col1 = type->createColumn();
    auto col2 = type->createColumn();
    auto col3 = type->createColumn();
    int rows = 500000;
    for (int i = 0; i < rows; ++i)
    {
        if (i % 9 != 0)
        {
            col1->insert(i);
            col2->insert(std::rand());
        }
        else
        {
            col1->insertDefault();
            col2->insertDefault();
        }
        col3->insert(std::rand());
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    Chunk chunk(std::move(columns), rows);


    Block header
        = {ColumnWithTypeAndName(type->createColumn(), type, "x"),
           ColumnWithTypeAndName(type->createColumn(), type, "y"),
           ColumnWithTypeAndName(type->createColumn(), type, "z")};
    auto path = "/tmp/test.parquet";
    writeParquet(std::move(chunk), header, path);

    ReadBufferFromFile in(path);
    auto reader = openParquet(header, in);
    //    reader->addFilter("x", std::make_shared<Int64RangeFilter>( 1000, 2000));
    int count = 0;
    int null_count2 = 0;
    while (auto block = reader->read())
    {
        if (block.rows() == 0)
            break;
        auto column2 = block.getByPosition(1).column;
        for (size_t i = 0; i < column2->size(); ++i)
        {
            if (column2->isNullAt(i))
                null_count2++;
        }
        count += block.rows();
    }
    ASSERT_EQ(count, 890);
    ASSERT_EQ(0, null_count2);
}

TEST(Processors, TestReadNullableFloat)
{
    auto float_type = makeNullable(std::make_shared<DataTypeFloat32>());
    auto double_type = makeNullable(std::make_shared<DataTypeFloat64>());

    auto col1 = float_type->createColumn();
    auto col2 = double_type->createColumn();
    auto col3 = double_type->createColumn();
    int rows = 500000;
    for (int i = 0; i < rows; ++i)
    {
        if (i % 9 != 0)
        {
            col1->insert(i * 0.1);
            col2->insert(std::rand() * 0.1);
        }
        else
        {
            col1->insertDefault();
            col2->insertDefault();
        }
        col3->insert(std::rand() * 0.1);
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    Chunk chunk(std::move(columns), rows);


    Block header
        = {ColumnWithTypeAndName(float_type->createColumn(), float_type, "x"),
           ColumnWithTypeAndName(double_type->createColumn(), double_type, "y"),
           ColumnWithTypeAndName(double_type->createColumn(), double_type, "z")};
    headBlock(header.cloneWithColumns(chunk.getColumns()), 20);
    auto path = "/tmp/test.parquet";
    writeParquet(std::move(chunk), header, path);

    ReadBufferFromFile in(path);
    auto reader = openParquet(header, in);
    //    reader->addFilter("x", std::make_shared<Int64RangeFilter>( 1000, 2000));
    int count = 0;
    int null_count2 = 0;
    bool first = true;
    while (auto block = reader->read())
    {
        if (block.rows() == 0)
            break;
        if (first)
        {
            headBlock(block, 20);
            first = false;
        }
        auto column2 = block.getByPosition(1).column;
        for (size_t i = 0; i < column2->size(); ++i)
        {
            if (column2->isNullAt(i))
                null_count2++;
        }
        count += block.rows();
    }
    ASSERT_EQ(count, 500000);
    ASSERT_EQ(55556, null_count2);
}

TEST(Processors, TestReadNullableString)
{
    auto string_type = makeNullable(std::make_shared<DataTypeString>());

    auto col1 = string_type->createColumn();
    auto col2 = string_type->createColumn();
    auto col3 = string_type->createColumn();
    int rows = 500000;
    for (int i = 0; i < rows; ++i)
    {
        if (i % 9 != 0)
        {
            col1->insert(std::to_string(i % 100));
            col2->insert(std::to_string(std::rand()));
        }
        else
        {
            col1->insertDefault();
            col2->insertDefault();
        }
        col3->insert(std::to_string(std::rand() * 0.1));
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    Chunk chunk(std::move(columns), rows);


    Block header
        = {ColumnWithTypeAndName(string_type->createColumn(), string_type, "x"),
           ColumnWithTypeAndName(string_type->createColumn(), string_type, "y"),
           ColumnWithTypeAndName(string_type->createColumn(), string_type, "z")};
    headBlock(header.cloneWithColumns(chunk.getColumns()), 20);
    auto path = "/tmp/test.parquet";
    writeParquet(std::move(chunk), header, path);

    ReadBufferFromFile in(path);
    auto reader = openParquet(header, in);
    reader->addFilter("x", std::make_shared<ByteValuesFilter>(std::vector<std::string>{"0", "1", "2", "3"}, false));
    int count = 0;
    int null_count2 = 0;
    bool first = true;
    while (auto block = reader->read())
    {
        if (block.rows() == 0)
            break;
        if (first)
        {
            headBlock(block, 20);
            first = false;
        }
        auto column2 = block.getByPosition(1).column;
        for (size_t i = 0; i < column2->size(); ++i)
        {
            if (column2->isNullAt(i))
                null_count2++;
        }
        count += block.rows();
    }
    ASSERT_EQ(count, 17779);
    ASSERT_EQ(0, null_count2);
}


TEST(Processors, BenchmarkReadNullableString)
{
    auto string_type = makeNullable(std::make_shared<DataTypeString>());

    auto col1 = string_type->createColumn();
    auto col2 = string_type->createColumn();
    auto col3 = string_type->createColumn();
    auto col4 = string_type->createColumn();
    auto col5 = string_type->createColumn();
    auto col6 = string_type->createColumn();
    auto col7 = string_type->createColumn();
    int rows = 5000000;
    for (int i = 0; i < rows; ++i)
    {
        if (i % 9 != 0)
        {
            col1->insert(std::to_string(i % 100));
            col2->insert(std::to_string(std::rand()));
        }
        else
        {
            col1->insertDefault();
            col2->insertDefault();
        }
        col3->insert(std::to_string(std::rand() * 0.1));
        col4->insert(std::to_string(std::rand() * 0.1));
        col5->insert(std::to_string(std::rand() * 0.1));
        col6->insert(std::to_string(std::rand() * 0.1));
        col7->insert(std::to_string(std::rand() * 0.1));
    }
    Columns columns;
    columns.emplace_back(std::move(col1));
    columns.emplace_back(std::move(col2));
    columns.emplace_back(std::move(col3));
    columns.emplace_back(std::move(col4));
    columns.emplace_back(std::move(col5));
    columns.emplace_back(std::move(col6));
    columns.emplace_back(std::move(col7));
    Chunk chunk(std::move(columns), rows);


    Block header = {
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "x"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "y"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "z"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "a"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "b"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "c"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "d"),
    };
    auto path = "/tmp/test.parquet";
    writeParquet(std::move(chunk), header, path);

    auto old_test [[maybe_unused]] = [&]()
    {
        ReadBufferFromFile in(path);
        testOldParquet(header, in);
    };

    auto new_test = [&]()
    {
        ReadBufferFromFile in(path);
        auto reader = openParquet(header, in);
        reader->addFilter(
            "x", std::make_shared<ByteValuesFilter>(std::vector<std::string>{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, false));
        int count [[maybe_unused]] = 0;
        while (auto block = reader->read())
        {
            count += block.rows();
            if (block.rows() == 0)
                break;
        }
        std::cerr << "total count: " << count << std::endl;
    };
    std::cerr << "start benchmark \n";
    benchmark("arrow", 21, old_test);
    benchmark("native", 21, new_test);
}

template<class T>
static void testGatherDictInt()
{
    PaddedPODArray<T> data = {0, 0, 0, 3, 3, 3, 4, 7, 0, 4, 7, 0, 9, 1, 5, 6, 7, 8, 9, 3, 4, 6, 7};
    PaddedPODArray<T> dict = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    PaddedPODArray<T> dist;
    PaddedPODArray<Int32> idx = {0, 0, 0, 3, 3, 3, 4, 7, 0, 4, 7, 0, 9, 1, 5, 6, 7, 8, 9, 3, 4, 6, 7};
    dist.reserve(data.size());
    FilterHelper::gatherDictFixedValue(dict, dist, idx, data.size());
    ASSERT_EQ(data.size(), dist.size());
    for (size_t i = 0; i < data.size(); ++i)
    {
        ASSERT_EQ(data[i], dist[i]);
    }
}

TEST(TestColumnFilterHepler, TestGatherDictNumberData)
{
    testGatherDictInt<Int16>();
    testGatherDictInt<Int32>();
    testGatherDictInt<Int64>();
    testGatherDictInt<Float32>();
    testGatherDictInt<Float64>();
    testGatherDictInt<DateTime64>();
}

TEST(TestRowSet, TestRowSet)
{
    RowSet rowSet(10000);
    rowSet.setAllFalse();
    rowSet.set(100, true);
    rowSet.set(1234, true);
    ASSERT_EQ(2, rowSet.count());
    ASSERT_FALSE(rowSet.none());
    ASSERT_TRUE(rowSet.any());
    ASSERT_FALSE(rowSet.all());
}
