#include <gtest/gtest.h>

#include <bit>

#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/ExternalDistinctTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

Chunk makeChunk(const std::vector<UInt64> & keys, const std::vector<UInt8> & flags)
{
    auto key_column = ColumnUInt64::create();
    for (auto key : keys)
        key_column->insertValue(key);

    auto flag_column = ColumnUInt8::create();
    for (auto flag : flags)
        flag_column->insertValue(flag);

    Columns columns;
    columns.emplace_back(std::move(key_column));
    columns.emplace_back(std::move(flag_column));
    return Chunk(std::move(columns), keys.size());
}

Chunk makeFloatChunk(const std::vector<Float64> & keys, const std::vector<UInt8> & flags)
{
    auto key_column = ColumnFloat64::create();
    for (auto key : keys)
        key_column->insertValue(key);

    auto flag_column = ColumnUInt8::create();
    for (auto flag : flags)
        flag_column->insertValue(flag);

    Columns columns;
    columns.emplace_back(std::move(key_column));
    columns.emplace_back(std::move(flag_column));
    return Chunk(std::move(columns), keys.size());
}

DistinctSortedFilter makeFilter()
{
    SortDescription description;
    description.emplace_back("k", 1, 1);
    return DistinctSortedFilter({0}, description, 1);
}

std::vector<UInt64> extractKeys(const Chunk & chunk)
{
    const auto & data = assert_cast<const ColumnUInt64 &>(*chunk.getColumns()[0]).getData();
    return {data.begin(), data.end()};
}

std::vector<Float64> extractFloatKeys(const Chunk & chunk)
{
    const auto & data = assert_cast<const ColumnFloat64 &>(*chunk.getColumns()[0]).getData();
    return {data.begin(), data.end()};
}

}

TEST(DistinctSortedFilter, DeduplicatesWithinChunk)
{
    auto filter = makeFilter();
    auto result = filter.filter(makeChunk({1, 1, 2, 3, 3, 3}, {0, 0, 0, 0, 0, 0}), /*strip_flag=*/ false);

    EXPECT_EQ(result.getNumColumns(), 2u);
    EXPECT_EQ(extractKeys(result), (std::vector<UInt64>{1, 2, 3}));
}

TEST(DistinctSortedFilter, StripsFlagColumn)
{
    auto filter = makeFilter();
    auto result = filter.filter(makeChunk({1, 2}, {0, 0}), /*strip_flag=*/ true);

    EXPECT_EQ(result.getNumColumns(), 1u);
    EXPECT_EQ(extractKeys(result), (std::vector<UInt64>{1, 2}));
}

TEST(DistinctSortedFilter, ContinuesRangeAcrossChunks)
{
    auto filter = makeFilter();

    auto first = filter.filter(makeChunk({1, 2, 2}, {0, 0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractKeys(first), (std::vector<UInt64>{1, 2}));

    /// The first row continues the range of key 2, so it must be suppressed.
    auto second = filter.filter(makeChunk({2, 3}, {0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractKeys(second), (std::vector<UInt64>{3}));
}

TEST(DistinctSortedFilter, ResetForgetsPreviousChunk)
{
    auto filter = makeFilter();

    auto first = filter.filter(makeChunk({1, 2}, {0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractKeys(first), (std::vector<UInt64>{1, 2}));

    filter.reset();

    /// Without reset the leading 2 would be treated as a continuation of the previous range.
    auto second = filter.filter(makeChunk({2, 3}, {0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractKeys(second), (std::vector<UInt64>{2, 3}));
}

TEST(DistinctSortedFilter, FlaggedRowSuppressesItsKey)
{
    auto filter = makeFilter();

    /// Key 1 was already emitted (flag on its first row): the whole group is suppressed.
    /// Key 2 was not: its first row is emitted once.
    auto result = filter.filter(makeChunk({1, 1, 2, 2}, {1, 0, 0, 0}), /*strip_flag=*/ true);
    EXPECT_EQ(extractKeys(result), (std::vector<UInt64>{2}));
}

TEST(DistinctSortedFilter, FlaggedRowSuppressesAcrossChunks)
{
    auto filter = makeFilter();

    auto first = filter.filter(makeChunk({1}, {1}), /*strip_flag=*/ true);
    EXPECT_EQ(first.getNumRows(), 0u);

    auto second = filter.filter(makeChunk({1, 2}, {0, 0}), /*strip_flag=*/ true);
    EXPECT_EQ(extractKeys(second), (std::vector<UInt64>{2}));
}

TEST(DistinctSortedFilter, EmptyOutputChunk)
{
    auto filter = makeFilter();

    auto first = filter.filter(makeChunk({7, 7}, {0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractKeys(first), (std::vector<UInt64>{7}));

    auto second = filter.filter(makeChunk({7, 7, 7}, {0, 0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(second.getNumRows(), 0u);
}

TEST(DistinctSortedFilter, SortEqualRowsCollapse)
{
    /// 0. and -0. compare equal in the sort order: after the spill they are deduplicated as one value
    /// (like DISTINCT in order does), even though the in-memory hash DISTINCT distinguishes them by
    /// the binary representation.
    auto filter = makeFilter();
    auto result = filter.filter(makeFloatChunk({-0., 0., 0.}, {0, 0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(extractFloatKeys(result).size(), 1u);
}

TEST(DistinctSortedFilter, NaNsCollapse)
{
    /// All NaNs compare equal in the sort order regardless of the payload.
    const Float64 nan1 = std::numeric_limits<Float64>::quiet_NaN();
    const Float64 nan2 = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan1) ^ 1);

    auto filter = makeFilter();
    auto result = filter.filter(makeFloatChunk({nan1, nan1, nan2}, {0, 0, 0}), /*strip_flag=*/ false);
    EXPECT_EQ(result.getNumRows(), 1u);
}

TEST(DistinctSortedFilter, FlagSuppressesWholeEqualRange)
{
    /// -0. was emitted before the spill: the whole range of equal rows is suppressed, including 0.
    /// (a value class that was started before the spill keeps the in-memory result; classes first seen
    /// after the spill are deduplicated by the sort comparison).
    auto filter = makeFilter();
    auto result = filter.filter(makeFloatChunk({-0., -0., 0.}, {1, 0, 0}), /*strip_flag=*/ true);
    EXPECT_EQ(result.getNumRows(), 0u);
}

TEST(DistinctSortedFilter, MergeTieBreakKeepsFirstInputFirst)
{
    /// The suppression of the already-emitted rows relies on MergingSortedTransform returning the rows
    /// of input 0 before the equal rows of the other inputs (the sorting queues break ties by the input
    /// index, see the note on SortCursorHelper in Core/SortCursor.h). This test pins that contract with
    /// a real merge: input 0 is the flagged "already emitted" run, input 1 shares some of its keys.

    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "flag")};
    const auto shared_header = std::make_shared<const Block>(header);

    auto make_source = [&](const std::vector<std::vector<UInt64>> & runs, UInt8 flag)
    {
        Chunks chunks;
        for (const auto & keys : runs)
            chunks.push_back(makeChunk(keys, std::vector<UInt8>(keys.size(), flag)));
        return std::make_shared<SourceFromChunks>(shared_header, std::move(chunks));
    };

    /// Keys 1..6 were emitted before the spill (input 0); keys 2, 4, 6, 7 arrive from a later run.
    auto emitted_run = make_source({{1, 2, 3}, {4, 5, 6}}, 1);
    auto later_run = make_source({{2, 4}, {6, 7}}, 0);

    SortDescription description;
    description.emplace_back("k", 1, 1);

    auto merge = std::make_shared<MergingSortedTransform>(
        shared_header,
        /*num_inputs=*/ 2,
        description,
        /*max_block_size_rows=*/ 3,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch);

    connect(emitted_run->getPort(), merge->getInputs().front());
    connect(later_run->getPort(), merge->getInputs().back());

    auto * output_port = &merge->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(emitted_run));
    processors->emplace_back(std::move(later_run));
    processors->emplace_back(std::move(merge));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    PullingPipelineExecutor executor(pipeline);

    auto filter = makeFilter();
    std::vector<UInt64> distinct_keys;
    std::optional<UInt64> prev_key;
    UInt8 prev_flag = 1;

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        const auto & keys = assert_cast<const ColumnUInt64 &>(*block.getByPosition(0).column).getData();
        const auto & flags = assert_cast<const ColumnUInt8 &>(*block.getByPosition(1).column).getData();

        /// The contract itself: within a group of equal keys, flagged rows come first.
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (prev_key && *prev_key == keys[i])
                EXPECT_LE(flags[i], prev_flag) << "flagged row after an equal unflagged row, key " << keys[i];
            prev_key = keys[i];
            prev_flag = flags[i];
        }

        /// And its consequence: the filter must emit exactly the keys that were not emitted before.
        auto filtered = filter.filter(Chunk(block.getColumns(), block.rows()), /*strip_flag=*/ false);
        if (filtered.hasRows())
        {
            const auto & filtered_keys = assert_cast<const ColumnUInt64 &>(*filtered.getColumns()[0]).getData();
            distinct_keys.insert(distinct_keys.end(), filtered_keys.begin(), filtered_keys.end());
        }
    }

    EXPECT_EQ(distinct_keys, (std::vector<UInt64>{7}));
}

TEST(DistinctSortedFilter, SortEqualZerosCollapseThroughMerge)
{
    /// The post-spill deduplication must collapse values that compare equal in the sort order but
    /// differ in the binary representation (-0. and 0.) also when they come through a real merge of
    /// a flagged run and a later run, not only within a hand-built chunk.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeFloat64>(), "k"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "flag")};
    const auto shared_header = std::make_shared<const Block>(header);

    auto make_source = [&](const std::vector<Float64> & keys, UInt8 flag)
    {
        Chunks chunks;
        chunks.push_back(makeFloatChunk(keys, std::vector<UInt8>(keys.size(), flag)));
        return std::make_shared<SourceFromChunks>(shared_header, std::move(chunks));
    };

    /// The flagged run holds only fillers; the later run starts with the two zero representatives.
    auto emitted_run = make_source({100., 101.}, 1);
    auto later_run = make_source({-0., 0., 100., 102.}, 0);

    SortDescription description;
    description.emplace_back("k", 1, 1);

    auto merge = std::make_shared<MergingSortedTransform>(
        shared_header,
        /*num_inputs=*/ 2,
        description,
        /*max_block_size_rows=*/ 3,
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns=*/ std::nullopt,
        SortingQueueStrategy::Batch);

    connect(emitted_run->getPort(), merge->getInputs().front());
    connect(later_run->getPort(), merge->getInputs().back());

    auto * output_port = &merge->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(emitted_run));
    processors->emplace_back(std::move(later_run));
    processors->emplace_back(std::move(merge));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    PullingPipelineExecutor executor(pipeline);

    auto filter = makeFilter();
    std::vector<Float64> distinct_keys;

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        auto filtered = filter.filter(Chunk(block.getColumns(), block.rows()), /*strip_flag=*/ false);
        if (filtered.hasRows())
        {
            const auto & keys = assert_cast<const ColumnFloat64 &>(*filtered.getColumns()[0]).getData();
            distinct_keys.insert(distinct_keys.end(), keys.begin(), keys.end());
        }
    }

    /// Exactly one zero (the first-received -0.) and the new filler 102.
    EXPECT_EQ(distinct_keys.size(), 2u);
}
