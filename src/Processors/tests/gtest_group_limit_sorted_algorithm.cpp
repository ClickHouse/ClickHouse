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
    /// rows one by one. The seen counts of an unbounded window need 64 bits.
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
            {header}, std::make_shared<const Block>(output_header), keyDescription(),
            NameAndTypePair{"seen_rows", header->getByName("seen_rows").type}, offset, end, 2);
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
        {header}, std::make_shared<const Block>(output_header), keyDescription(),
        NameAndTypePair{"seen_rows", header->getByName("seen_rows").type}, /*group_offset_=*/ 0,
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

TEST(GroupLimitSortedAlgorithm, InputRowsWithoutSeenCountsRankAfterSeenRows)
{
    /// Input runs store no seen counts. The merge adds zero counts of the seen-count type, so input rows rank
    /// after the seen rows of their key, in the window of `DISTINCT` and in a ranked window.
    const auto seen_type = std::make_shared<DataTypeUInt8>();
    const auto seen_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"), ColumnWithTypeAndName(seen_type, "seen_rows")});
    const auto input_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});

    struct Case
    {
        UInt64 offset;
        UInt64 end;
        std::vector<UInt64> input_keys;
        std::vector<std::pair<UInt64, UInt64>> expected;
    };
    /// Key 1 was seen once before spilling. The input rows hold payloads from 10, at most the window end of
    /// rows of a key.
    const std::vector<Case> cases{
        {0, 1, {1, 2}, {{2, 11}}},
        {1, 3, {1, 1, 2, 2}, {{1, 10}, {1, 11}, {2, 13}}},
    };
    for (const auto & [offset, end, input_keys, expected] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << ")");
        GroupLimitSortedAlgorithm algorithm(
            {seen_header, input_header}, input_header, keyDescription(), NameAndTypePair{"seen_rows", seen_type}, offset, end, 64);
        IMergingAlgorithm::Inputs inputs(2);
        auto seen_columns = seen_header->cloneEmptyColumns();
        seen_columns[0]->insert(1u);
        seen_columns[1]->insert(1u);
        inputs[0].chunk = Chunk(std::move(seen_columns), 1);
        auto input_columns = input_header->cloneEmptyColumns();
        UInt64 payload = 10;
        for (const auto key : input_keys)
        {
            input_columns[0]->insert(key);
            input_columns[1]->insert(payload++);
        }
        inputs[1].chunk = Chunk(std::move(input_columns), input_keys.size());
        algorithm.initialize(std::move(inputs));

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
