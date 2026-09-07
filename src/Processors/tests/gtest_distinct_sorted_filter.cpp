#include <gtest/gtest.h>

#include <array>
#include <bit>

#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/DistinctSortedFilter.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/assert_cast.h>

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
    /// 0. and -0. compare equal in the sort order: after the spill they are deduplicated as one value (like
    /// `DISTINCT` in order does), even though the in-memory hash `DISTINCT` distinguishes them by the
    /// binary representation.
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

TEST(DistinctSortedFilter, MergeSuppressionOrderKeepsFirstOrdinaryPayload)
{
    /// Suppression runs may be registered anywhere. Ordinary runs retain chronological registration
    /// so equal keys keep the first ordinary payload. A user payload name also exercises flag renaming.
    const auto input_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "__distinct_already_emitted")});
    const DistinctSpillLayout layout(input_header, {0}, /*preserve_input_order=*/ false);
    const auto & spill_header = layout.getSpillHeader();

    auto make_source = [&](const std::vector<std::vector<UInt64>> & runs, bool suppression, UInt64 payload)
    {
        Chunks chunks;
        for (const auto & keys : runs)
        {
            auto key_column = ColumnUInt64::create();
            for (const auto key : keys)
                key_column->insertValue(key);

            if (suppression)
            {
                MutableColumns columns;
                columns.emplace_back(std::move(key_column));
                chunks.push_back(layout.prepareSuppressionChunk(std::move(columns)));
            }
            else
            {
                auto payload_column = ColumnUInt64::create();
                for (size_t row = 0; row < keys.size(); ++row)
                    payload_column->insertValue(payload++);

                Columns columns;
                columns.emplace_back(std::move(key_column));
                columns.emplace_back(std::move(payload_column));
                chunks.push_back(layout.prepareInputChunk(Chunk(std::move(columns), keys.size()), 0));
            }
        }
        return std::make_shared<SourceFromChunks>(spill_header, std::move(chunks));
    };

    for (const auto & input_order : {
             std::array<size_t, 4>{0, 1, 2, 3},
             std::array<size_t, 4>{1, 0, 3, 2},
             std::array<size_t, 4>{0, 2, 3, 1}})
    {
        SCOPED_TRACE(::testing::PrintToString(input_order));
        std::array sources{
            make_source({{2, 4, 7}, {7, 8}}, false, 100),
            make_source({{1, 2}, {4}}, true, 0),
            make_source({{2, 4, 7}, {8, 9}}, false, 200),
            make_source({{3}, {5, 6}}, true, 0)};

        auto merge = std::make_shared<MergingSortedTransform>(
            spill_header,
            sources.size(),
            layout.getRunSortDescription(),
            /*max_block_size_rows=*/ 2,
            /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt,
            SortingQueueStrategy::Batch);

        auto processors = std::make_shared<Processors>();
        auto input = merge->getInputs().begin();
        for (const auto source_index : input_order)
        {
            auto & source = sources[source_index];
            connect(source->getPort(), *input++);
            processors->emplace_back(std::move(source));
        }
        auto * output_port = &merge->getOutputs().front();
        processors->emplace_back(std::move(merge));

        QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
        PullingPipelineExecutor executor(pipeline);
        DistinctSortedFilter filter(
            layout.getKeyColumnsPositions(), layout.getKeySortDescription(), layout.getFlagColumnPosition());
        std::vector<std::pair<UInt64, UInt64>> distinct_rows;
        std::optional<UInt64> prev_key;
        UInt8 prev_flag = 1;
        bool crossed_chunk_boundary = false;

        Block block;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            const auto & keys = assert_cast<const ColumnUInt64 &>(*block.getByPosition(0).column).getData();
            const auto & flags = assert_cast<const ColumnUInt8 &>(
                *block.getByPosition(layout.getFlagColumnPosition()).column).getData();

            crossed_chunk_boundary |= prev_key && *prev_key == keys.front();
            /// Suppression priority holds across output chunks as well as within each chunk.
            for (size_t row = 0; row < keys.size(); ++row)
            {
                if (prev_key && *prev_key == keys[row])
                    EXPECT_LE(flags[row], prev_flag) << "flagged row after an unflagged row, key " << keys[row];
                prev_key = keys[row];
                prev_flag = flags[row];
            }

            auto filtered = filter.filter(Chunk(block.getColumns(), block.rows()), /*strip_flag=*/ true);
            const auto & filtered_keys = assert_cast<const ColumnUInt64 &>(*filtered.getColumns()[0]).getData();
            const auto & payloads = assert_cast<const ColumnUInt64 &>(*filtered.getColumns()[1]).getData();
            for (size_t row = 0; row < filtered_keys.size(); ++row)
                distinct_rows.emplace_back(filtered_keys[row], payloads[row]);
        }

        EXPECT_TRUE(crossed_chunk_boundary);
        EXPECT_EQ(distinct_rows, (std::vector<std::pair<UInt64, UInt64>>{{7, 102}, {8, 104}, {9, 204}}));
    }
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
