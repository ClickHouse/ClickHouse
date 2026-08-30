#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Processors/Sources/BlocksListSource.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/IProcessor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Port.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int RECEIVED_EMPTY_DATA;
}

static Block getBlockWithSize(const std::vector<std::string> & columns, size_t rows, size_t stride, size_t & start)
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


static Pipe getInputStreams(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = stride;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
    }
    return Pipe::unitePipes(std::move(pipes));

}


static Pipe getInputStreamsEqualStride(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;
    size_t i = 0;
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = i;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
        i++;
    }
    return Pipe::unitePipes(std::move(pipes));

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

TEST(MergingSortedTest, SimpleBlockSizeTest)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreams(key_columns, {{5, 1, 1}, {10, 1, 2}, {21, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);

    auto transform = std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        sort_description,
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block1;
    Block block2;
    Block block3;
    executor.pull(block1);
    executor.pull(block2);
    executor.pull(block3);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    for (const auto & block : {block1, block2, block3})
        total_rows += block.rows();

    /**
      * First block consists of 1 row from block3 with 21 rows + 2 rows from block2 with 10 rows
      * + 5 rows from block 1 with 5 rows granularity
      */
    EXPECT_EQ(block1.rows(), 8);
    /**
      * Second block consists of 8 rows from block2 + 6 rows from block3.
      */
    EXPECT_EQ(block2.rows(), 14);
    /**
      * Third block consists of the remaining 14 rows from block3.
      */
    EXPECT_EQ(block3.rows(), 14);

    EXPECT_EQ(total_rows, 5 + 10 + 21);
}


TEST(MergingSortedTest, MoreInterestingBlockSizes)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreamsEqualStride(key_columns, {{1000, 1, 3}, {1500, 1, 3}, {1400, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);

    auto transform = std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        sort_description,
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Block block1;
    Block block2;
    Block block3;
    executor.pull(block1);
    executor.pull(block2);
    executor.pull(block3);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    EXPECT_EQ(block1.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block2.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block3.rows(), (1000 + 1500 + 1400) / 3);

    EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 1000 + 1500 + 1400);
}


namespace
{

/// Pushes the given blocks in order, sending a chunk that carries the header columns and no rows in
/// place of every empty entry, and finishing the port in the same `prepare()` as the last entry. A
/// port whose header is not empty cannot carry "no data" as a columnless chunk, which is why
/// `ISimpleTransform::work` builds this shape.
class ScriptedSource : public IProcessor
{
public:
    ScriptedSource(const Block & header, std::vector<Block> script_)
        : IProcessor({}, {std::make_shared<const Block>(header.cloneEmpty())})
        , output(outputs.front())
        , script(std::move(script_))
    {
    }

    String getName() const override { return "ScriptedSource"; }

    Status prepare() override
    {
        if (output.isFinished())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (pos < script.size())
        {
            const auto & block = script[pos];
            ++pos;
            if (block.rows() == 0)
                output.push(Chunk(output.getHeader().cloneEmpty().getColumns(), 0));
            else
                output.push(Chunk(block.getColumns(), block.rows()));
        }

        if (pos < script.size())
            return Status::PortFull;

        output.finish();
        return Status::Finished;
    }

private:
    OutputPort & output;
    std::vector<Block> script;
    size_t pos = 0;
};

}

/// Merges one `ScriptedSource` per script and returns the key column values in the order the merge
/// produced them.
static std::vector<UInt64> mergeKeys(const Block & header, const std::vector<std::vector<Block>> & scripts)
{
    Pipes pipes;
    for (const auto & script : scripts)
        pipes.emplace_back(std::make_shared<ScriptedSource>(header, script));
    auto pipe = Pipe::unitePipes(std::move(pipes));

    pipe.addTransform(std::make_shared<MergingSortedTransform>(
        pipe.getSharedHeader(),
        pipe.numOutputPorts(),
        getSortDescription({"K1"}),
        /*max_block_size_rows=*/ 8192,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        /*limit=*/ 0,
        /*always_read_till_end=*/ false,
        /*out_row_sources_buf=*/ nullptr,
        /*filter_column_name=*/ std::nullopt,
        /*use_average_block_sizes=*/ false));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    std::vector<UInt64> keys;
    Block block;
    while (executor.pull(block))
    {
        const auto & column = *block.getByName("K1").column;
        for (size_t i = 0; i < column.size(); ++i)
            keys.push_back(column.getUInt(i));
    }
    return keys;
}

/// A chunk with no rows is not data for a merge: a cursor over it has no row to read. Here it is the
/// last chunk of the last live source, so it reaches the merge with an empty queue, which is the case
/// `gtest_merging_sorted_empty_chunk.cpp` also covers, checked here on the merged keys.
TEST(MergingSortedTest, RowlessChunkFromFinishedInput)
{
    std::vector<std::string> key_columns{"K1"};
    size_t start = 0;
    auto first = getBlockWithSize(key_columns, 4, 1, start);
    start = 100;
    auto second = getBlockWithSize(key_columns, 4, 1, start);

    auto keys = mergeKeys(first, {{first}, {second, Block{}}});

    /// The rowless chunk must not displace the real data of the source that pushed it.
    EXPECT_EQ(keys, (std::vector<UInt64>{0, 1, 2, 3, 100, 101, 102, 103}));
}

/// A rowless chunk from a source that has NOT finished only means "no data yet": the merge must wait
/// for that source instead of treating it as exhausted, or the rows it still owes are lost.
/// `gtest_merging_sorted_empty_chunk.cpp` sends its rowless chunk last, so only this arm pins the wait.
TEST(MergingSortedTest, RowlessChunkFromUnfinishedInput)
{
    std::vector<std::string> key_columns{"K1"};
    size_t start = 0;
    auto first = getBlockWithSize(key_columns, 4, 1, start);
    start = 100;
    auto second = getBlockWithSize(key_columns, 2, 1, start);
    auto third = getBlockWithSize(key_columns, 2, 1, start);

    auto keys = mergeKeys(first, {{first}, {second, Block{}, third}});

    EXPECT_EQ(keys, (std::vector<UInt64>{0, 1, 2, 3, 100, 101, 102, 103}));
}

namespace
{

struct GatherResult
{
    size_t rows = 0;
    /// Error code the merge threw, 0 if it did not throw.
    int code = 0;
    /// Whether the merge reported completion, as opposed to still asking for a source at the step bound.
    bool finished = false;
};

}

/// Steps a real `ColumnGathererTransform` through its ports the way the executor does, for a bounded
/// number of rounds. Each source is given its one block and then finished, so a source the merge asks
/// for again is exhausted: a port reports finished only once its data has been pulled.
static GatherResult gatherRows(const std::vector<Block> & blocks, const std::vector<size_t> & row_sources)
{
    /// One round per mask entry plus a few per source finishes every case here, with room to spare.
    static constexpr size_t max_rounds = 1000;

    std::string mask;
    for (size_t source : row_sources)
        mask.push_back(static_cast<char>(RowSourcePart(source).data));

    auto header = std::make_shared<const Block>(blocks.front().cloneEmpty());
    auto transform = std::make_shared<ColumnGathererTransform>(
        header,
        blocks.size(),
        std::make_unique<ReadBufferFromOwnString>(mask),
        /*block_preferred_size_rows_=*/ 8192,
        /*block_preferred_size_bytes_=*/ 1UL << 30,
        /*max_dynamic_subcolumns_=*/ std::nullopt,
        /*is_result_sparse_=*/ false);
    IProcessor * processor = transform.get();

    std::vector<std::unique_ptr<OutputPort>> feeders;
    auto input_it = transform->getInputs().begin();
    for (size_t i = 0; i < blocks.size(); ++i, ++input_it)
    {
        feeders.emplace_back(std::make_unique<OutputPort>(header));
        connect(*feeders.back(), *input_it);
        feeders.back()->push(Chunk(blocks[i].getColumns(), blocks[i].rows()));
        feeders.back()->finish();
    }

    auto consumer = std::make_unique<InputPort>(transform->getOutputs().front().getSharedHeader());
    connect(transform->getOutputs().front(), *consumer);

    GatherResult result;
    try
    {
        for (size_t round = 0; round < max_rounds; ++round)
        {
            consumer->setNeeded();
            auto status = processor->prepare();

            while (consumer->hasData())
            {
                consumer->setNeeded();
                result.rows += consumer->pull().getNumRows();
            }

            if (status == IProcessor::Status::Finished)
            {
                result.finished = true;
                break;
            }

            if (status == IProcessor::Status::Ready)
                processor->work();
        }
    }
    catch (const Exception & e)
    {
        result.code = e.code();
    }
    return result;
}

/// A single source read without a mask is passed through block by block, so its exhaustion ends the
/// result instead of leaving it short.
TEST(ColumnGathererTest, SingleSourcePassThrough)
{
    std::vector<std::string> key_columns{"K1"};
    size_t start = 0;

    auto result = gatherRows({getBlockWithSize(key_columns, 3, 1, start)}, {});

    EXPECT_TRUE(result.finished) << "the merge kept asking for the exhausted source";
    EXPECT_EQ(result.code, 0);
    EXPECT_EQ(result.rows, 3u);
}

/// With a mask, a source is re-requested only while the mask still maps rows to it, so a source that
/// is exhausted before delivering those rows must fail the merge rather than be asked again.
TEST(ColumnGathererTest, RequiredSourceExhausted)
{
    std::vector<std::string> key_columns{"K1"};
    size_t start = 0;
    auto first = getBlockWithSize(key_columns, 2, 1, start);
    start = 100;
    auto second = getBlockWithSize(key_columns, 2, 1, start);

    /// Alternate the sources so that gather() cannot copy a whole block at once, then ask for one
    /// row more from the second source than it delivered.
    auto code = gatherRows({first, second}, {0, 1, 0, 1, 1}).code;

    EXPECT_EQ(code, DB::ErrorCodes::RECEIVED_EMPTY_DATA);
}

/// A mask is what makes a shortage detectable, so one source with rows still mapped to it fails even
/// though the pass-through case above has the same source count.
TEST(ColumnGathererTest, SingleSourceWithRemainingMask)
{
    std::vector<std::string> key_columns{"K1"};
    size_t start = 0;
    auto only = getBlockWithSize(key_columns, 2, 1, start);

    auto code = gatherRows({only}, {0, 0, 0}).code;

    EXPECT_EQ(code, DB::ErrorCodes::RECEIVED_EMPTY_DATA);
}
