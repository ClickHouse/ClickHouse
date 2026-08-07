#include <Core/Block.h>
#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Processors/Sources/BlocksListSource.h>
#include <Processors/Transforms/CheckSortedTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <DataTypes/DataTypesNumber.h>

#include <atomic>
#include <chrono>
#include <thread>


using namespace DB;


namespace DB::FailPoints
{
extern const char check_sorted_transform_pause[];
extern const char check_sorted_transform_after_loop_pause[];
}


static SortDescription getSortDescription(const std::vector<std::string> & column_names)
{
    SortDescription descr;
    for (const auto & column : column_names)
    {
        descr.emplace_back(column, 1, 1);
    }
    return descr;
}

static Block getSortedBlockWithSize(
    const std::vector<std::string> & columns,
    size_t rows, size_t stride, size_t start)
{
    ColumnsWithTypeAndName cols;
    size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);
    for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; ++i)
    {
        auto column = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
        {
            column->getElement(j) = start;
            start += stride;
        }
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);
    }
    return Block(cols);
}


static Block getUnSortedBlockWithSize(const std::vector<std::string> & columns, size_t rows, size_t stride, size_t start, size_t bad_row, size_t bad_column, size_t bad_value)
{
    ColumnsWithTypeAndName cols;
    size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);
    for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; ++i)
    {
        auto column = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
        {
            if (bad_row == j && bad_column == i)
                column->getElement(j) = bad_value;
            else if (i < bad_column)
                column->getElement(j) = 0;
            else
                column->getElement(j) = start;

            start += stride;
        }
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);
    }
    return Block(cols);
}

static Block getEqualValuesBlockWithSize(
    const std::vector<std::string> & columns, size_t rows)
{
    ColumnsWithTypeAndName cols;
    size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);
    for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; ++i)
    {
        auto column = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
            column->getElement(j) = 0;

        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);
    }
    return Block(cols);
}


TEST(CheckSortedTransform, CheckGoodCase)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);

    BlocksList blocks;
    for (size_t i = 0; i < 3; ++i)
        blocks.push_back(getSortedBlockWithSize(key_columns, 10, 1, i * 10));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_FALSE(executor.pull(chunk));
}

TEST(CheckSortedTransform, CheckBadLastRow)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getSortedBlockWithSize(key_columns, 100, 1, 100));
    blocks.push_back(getSortedBlockWithSize(key_columns, 100, 1, 200));
    blocks.push_back(getSortedBlockWithSize(key_columns, 100, 1, 0));
    blocks.push_back(getSortedBlockWithSize(key_columns, 100, 1, 300));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_NO_THROW(executor.pull(chunk));

#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(executor.pull(chunk), DB::Exception);
#endif
}


TEST(CheckSortedTransform, CheckUnsortedBlock1)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getUnSortedBlockWithSize(key_columns, 100, 1, 0, 5, 1, 77));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;

#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(executor.pull(chunk), DB::Exception);
#endif
}

TEST(CheckSortedTransform, CheckUnsortedBlock2)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getUnSortedBlockWithSize(key_columns, 100, 1, 0, 99, 2, 77));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(executor.pull(chunk), DB::Exception);
#endif
}

TEST(CheckSortedTransform, CheckUnsortedBlock3)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getUnSortedBlockWithSize(key_columns, 100, 1, 0, 50, 0, 77));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(executor.pull(chunk), DB::Exception);
#endif
}

TEST(CheckSortedTransform, CheckEqualBlock)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getEqualValuesBlockWithSize(key_columns, 100));
    blocks.push_back(getEqualValuesBlockWithSize(key_columns, 10));
    blocks.push_back(getEqualValuesBlockWithSize(key_columns, 1));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    QueryPipeline pipeline(std::move(pipe));

    PullingPipelineExecutor executor(pipeline);

    Chunk chunk;
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_NO_THROW(executor.pull(chunk));
    EXPECT_NO_THROW(executor.pull(chunk));
}

namespace
{

/// Check that a `CheckSortedTransform` paused at a failpoint is interrupted promptly when the
/// pipeline is cancelled: the in-flight `pull` must finish without producing the chunk.
void checkCancellation(const char * fail_point_name)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    BlocksList blocks;
    blocks.push_back(getSortedBlockWithSize(key_columns, 10000, 1, 0));

    Pipe pipe(std::make_shared<BlocksListSource>(std::move(blocks)));
    pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<CheckSortedTransform>(header, sort_description);
    });

    auto pipeline = std::make_shared<QueryPipeline>(std::move(pipe));
    auto executor = std::make_shared<PullingPipelineExecutor>(*pipeline);

    FailPointInjection::enableFailPoint(fail_point_name);

    std::atomic<bool> pull_finished{false};
    std::atomic<bool> pull_result{true};
    /// The thread keeps `pipeline` and `executor` alive via the shared pointers, so on timeout it
    /// can be detached instead of joined: the failpoint is already disabled, so it terminates on
    /// its own without outliving the pipeline objects.
    std::thread pull_thread([&, executor, pipeline]
    {
        Chunk chunk;
        try
        {
            pull_result = executor->pull(chunk);
        }
        catch (...)
        {
            pull_result = false;
        }
        pull_finished = true;
    });

    FailPointInjection::waitForPause(fail_point_name);

    executor->cancel();
    FailPointInjection::disableFailPoint(fail_point_name);

    Stopwatch timer;
    while (!pull_finished && timer.elapsedSeconds() < 10)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    if (pull_finished)
        pull_thread.join();
    else
        pull_thread.detach();

    ASSERT_TRUE(pull_finished) << "Pull did not finish promptly after cancellation";
    EXPECT_FALSE(pull_result);
}

}

TEST(CheckSortedTransform, CheckCancellationBeforeLoop)
{
    checkCancellation(FailPoints::check_sorted_transform_pause);
}

TEST(CheckSortedTransform, CheckCancellationAfterLoop)
{
    checkCancellation(FailPoints::check_sorted_transform_after_loop_pause);
}
