#include <gtest/gtest.h>

#include <limits>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const DataTypePtr & seen_rows_type = std::make_shared<DataTypeUInt8>())
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload"),
        ColumnWithTypeAndName(seen_rows_type, "seen_rows")});
}

SortDescription keyDescription()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    return description;
}

Chunk makeChunk(const Block & header, const Array & keys, UInt64 first_payload, UInt64 seen_rows = 0)
{
    auto columns = header.cloneEmptyColumns();
    const size_t key_pos = header.getPositionByName("key");
    const size_t payload_pos = header.getPositionByName("payload");
    const size_t seen_rows_pos = header.getPositionByName("seen_rows");
    for (const auto & key : keys)
    {
        columns[key_pos]->insert(key);
        columns[payload_pos]->insert(first_payload++);
        columns[seen_rows_pos]->insert(seen_rows);
    }
    return Chunk(std::move(columns), keys.size());
}

}

TEST(GroupLimitSortedAlgorithm, SeenOnlyProgressIsBounded)
{
    /// Consumed seen rows count toward the block size, in the window of `DISTINCT` and in ranked windows.
    /// The ranks `[0, 1)` of `DISTINCT` and `LIMIT 1 BY` use the distinct merge path; any other window ranks
    /// rows one by one. `LIMIT BY` counts every row before spilling, so its seen counts are 64-bit.
    const auto limit_by_header = makeHeader(std::make_shared<DataTypeUInt64>());
    struct Case
    {
        SharedHeader header;
        UInt64 offset;
        UInt64 end;
        UInt64 seen_rows;
    };
    const std::vector<Case> cases{
        {makeHeader(), 0, 1, 1}, {limit_by_header, 0, 1, 7}, {limit_by_header, 1, 3, 2},
        {limit_by_header, 5, 7, std::numeric_limits<UInt64>::max()}};
    for (const auto & [header, offset, end, seen_rows] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << "), seen_rows=" << seen_rows);
        Block output_header = *header;
        output_header.erase("seen_rows");
        GroupLimitSortedAlgorithm algorithm(
            {header}, std::make_shared<const Block>(output_header), keyDescription(), "seen_rows", offset, end, 2);
        IMergingAlgorithm::Inputs inputs(1);
        inputs[0].chunk = makeChunk(*header, {1u, 1u, 2u, 3u, 4u}, 0, seen_rows);
        algorithm.initialize(std::move(inputs));
        for (size_t step = 0; step < 2; ++step)
        {
            auto status = algorithm.merge();
            EXPECT_FALSE(status.is_finished);
            EXPECT_EQ(status.required_source, -1);
            EXPECT_EQ(status.chunk.getNumRows(), 0);
            EXPECT_EQ(status.chunk.getNumColumns(), 2);
        }
        auto refill = algorithm.merge();
        EXPECT_EQ(refill.required_source, 0);
        EXPECT_TRUE(algorithm.merge().is_finished);
    }
}

TEST(GroupLimitSortedAlgorithm, WholeChunkOfDistinctKeysForwardsColumns)
{
    const auto header = makeHeader();
    Block output_header = *header;
    output_header.erase("seen_rows");
    GroupLimitSortedAlgorithm algorithm(
        {header}, std::make_shared<const Block>(output_header), keyDescription(), "seen_rows", /*group_offset_=*/ 0,
        /*group_limit_end_=*/ 1, 64);
    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk = makeChunk(*header, {1u, 2u, 3u}, 100);
    const auto * payload = inputs[0].chunk.getColumns()[1].get();
    algorithm.initialize(std::move(inputs));
    auto status = algorithm.merge();
    EXPECT_EQ(status.required_source, 0);
    EXPECT_EQ(status.chunk.getNumRows(), 3);
    EXPECT_EQ(status.chunk.getNumColumns(), 2);
    EXPECT_EQ(status.chunk.getColumns()[1].get(), payload);
    EXPECT_TRUE(algorithm.merge().is_finished);
}

TEST(GroupLimitSortedAlgorithm, WithoutSeenRowsEveryRowIsAnInputRow)
{
    /// Without the seen-rows column, as in the merge of the runs of a sort that keeps one row per key, every row
    /// takes a rank in its group: the distinct window keeps the first row of each key in source order, and a
    /// ranked window keeps the rows at its ranks.
    const auto header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});
    auto make_input = [&](const std::vector<UInt64> & keys, UInt64 first_payload)
    {
        auto columns = header->cloneEmptyColumns();
        for (const auto key : keys)
        {
            columns[0]->insert(key);
            columns[1]->insert(first_payload++);
        }
        return Chunk(std::move(columns), keys.size());
    };

    struct Case
    {
        UInt64 offset;
        UInt64 end;
        std::vector<UInt64> first_keys;
        std::vector<UInt64> second_keys;
        std::vector<std::pair<UInt64, UInt64>> expected;
    };
    /// The first input holds the payloads from 10, the second from 20, and equal keys take the first input first.
    /// An input holds at most the window end of rows of a key.
    const std::vector<Case> cases{
        {0, 1, {1, 2}, {1, 3}, {{1, 10}, {2, 11}, {3, 21}}},
        {1, 2, {1, 2}, {1, 2, 2}, {{1, 20}, {2, 21}}},
    };
    for (const auto & [offset, end, first_keys, second_keys, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << ")");
        GroupLimitSortedAlgorithm algorithm(
            {header, header}, header, keyDescription(), /*seen_rows_column=*/ std::nullopt, offset, end, 64);
        IMergingAlgorithm::Inputs inputs(2);
        inputs[0].chunk = make_input(first_keys, 10);
        inputs[1].chunk = make_input(second_keys, 20);
        algorithm.initialize(std::move(inputs));

        /// A request for the next chunk of an exhausted input needs no answer: the merge goes on without it.
        std::vector<std::pair<UInt64, UInt64>> rows;
        bool finished = false;
        for (size_t step = 0; step < 16 && !finished; ++step)
        {
            auto status = algorithm.merge();
            const auto & columns = status.chunk.getColumns();
            for (size_t row = 0; row < status.chunk.getNumRows(); ++row)
                rows.emplace_back(columns[0]->getUInt(row), columns[1]->getUInt(row));
            finished = status.is_finished;
        }
        EXPECT_TRUE(finished);
        EXPECT_EQ(rows, expected);
    }
}
