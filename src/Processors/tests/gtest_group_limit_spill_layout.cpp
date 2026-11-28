#include <gtest/gtest.h>

#include <limits>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/GroupLimitSpillLayout.h>
#include <QueryPipeline/SizeLimits.h>

using namespace DB;

namespace
{

SharedHeader makeKeyHeader()
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key")});
}

}

TEST(GroupLimitSpillLayout, SeenCountsTakeTheNarrowestTypeAndAreClampedToTheWindowEnd)
{
    /// Every count at or past the window end suppresses the rest of its group alike, so the counts are
    /// clamped to the end, in the narrowest unsigned type that holds it. A type of 64 bits holds every count
    /// as it is. Input runs carry no counts: the final merge counts their rows as unseen.
    struct Case
    {
        UInt64 group_limit_end;
        String type_name;
        std::vector<UInt64> counts;
    };
    const std::vector<Case> cases{
        {1, "UInt8", {1, 1, 1}},
        {255, "UInt8", {1, 255, 255}},
        {256, "UInt16", {1, 256, 256}},
        {70000, "UInt32", {1, 300, 70000}},
        {std::numeric_limits<UInt64>::max(), "UInt64", {1, 300, 100000}}};
    const auto header = makeKeyHeader();
    for (const auto & [group_limit_end, type_name, counts] : cases)
    {
        SCOPED_TRACE(group_limit_end);
        const GroupLimitSpillLayout layout(
            header, {0}, SpillKeyRepresentation::Columns, /*attach_arrival_numbers=*/ false, group_limit_end);
        const auto seen_rows = layout.getSeenRowsColumn();
        EXPECT_EQ(seen_rows.type->getName(), type_name);
        EXPECT_FALSE(layout.getInputRunHeader()->has(seen_rows.name));

        MutableColumns keys;
        keys.emplace_back(ColumnUInt64::create());
        auto seen_counts = ColumnUInt64::create();
        for (const UInt64 count : {1, 300, 100000})
        {
            keys.front()->insert(count);
            seen_counts->insertValue(count);
        }
        const auto chunk = layout.prepareSeenChunk(SeenGroups{std::move(keys), std::move(seen_counts)});
        const auto & seen_rows_column = chunk.getColumns()[layout.getSeenRunHeader()->getPositionByName(seen_rows.name)];
        EXPECT_EQ(seen_rows_column->getDataType(), seen_rows.type->getColumnType());
        ASSERT_EQ(seen_rows_column->size(), counts.size());
        for (size_t row = 0; row < counts.size(); ++row)
            EXPECT_EQ(seen_rows_column->getUInt(row), counts[row]);
    }
}

TEST(GroupLimitSpillLayout, SeenRunsContainOnlyRetainedKeys)
{
    /// Seen runs carry the comparison keys of the groups that the in-memory filter retained, without the
    /// payload: the columns of fixed-size keys, or the fingerprints of generic keys.
    constexpr size_t payload_size = 65536;
    for (const bool generic : {false, true})
    for (const bool attach_arrival_numbers : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "generic=" << generic << ", attach_arrival_numbers=" << attach_arrival_numbers);
        DataTypePtr key_type = std::make_shared<DataTypeUInt64>();
        if (generic)
            key_type = std::make_shared<DataTypeArray>(key_type);
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(key_type, "key"),
            ColumnWithTypeAndName(std::make_shared<DataTypeFixedString>(payload_size), "payload")});
        auto columns = header->cloneEmptyColumns();
        for (const UInt64 key : {1, 2, 1})
        {
            columns[0]->insert(generic ? Field(Array{Field(key)}) : Field(key));
            columns[1]->insertDefault();
        }
        DistinctSetFilter filter(*header, {"key"}, SizeLimits{});
        auto input = Chunk(std::move(columns), 3);
        filter.prepareForInsert(input);
        const auto representation = filter.getKeyRepresentation();
        EXPECT_EQ(representation, generic ? SpillKeyRepresentation::Hash128 : SpillKeyRepresentation::Columns);
        const GroupLimitSpillLayout layout(header, {0}, representation, attach_arrival_numbers, /*group_limit_end_=*/ 1);
        const auto input_run = layout.prepareInputChunk(input.clone(), 10);
        const auto emitted = filter.filter(std::move(input));
        ASSERT_EQ(emitted.getNumRows(), 2);

        auto extractor = std::move(filter).extractSeenGroups();
        const auto seen = layout.prepareSeenChunk(extractor->next(10, /*max_bytes=*/ 0));
        ASSERT_EQ(seen.getNumColumns(), 2);
        ASSERT_EQ(seen.getNumRows(), 2);
        EXPECT_LT(seen.allocatedBytes(), payload_size);
        EXPECT_FALSE(layout.getSeenRunHeader()->has("payload"));

        const auto & key_name = layout.getKeySortDescription().front().column_name;
        const auto & input_keys = *input_run.getColumns()[layout.getInputRunHeader()->getPositionByName(key_name)];
        const auto & retained_keys = *seen.getColumns()[layout.getSeenRunHeader()->getPositionByName(key_name)];
        EXPECT_NE(retained_keys.compareAt(0, 1, retained_keys, 1), 0);
        for (size_t row = 0; row < 2; ++row)
            EXPECT_TRUE(retained_keys.compareAt(row, 0, input_keys, 1) == 0 || retained_keys.compareAt(row, 1, input_keys, 1) == 0);
    }
}

TEST(GroupLimitSpillLayout, ServiceColumnsMatchActualAllocation)
{
    /// Input runs append the service columns, arrival numbers and fingerprints, to the spilled input columns,
    /// here only the key. Their estimate lets hashing reserve the memory to start spilling.
    const auto header = makeKeyHeader();
    for (const auto representation : {SpillKeyRepresentation::Columns, SpillKeyRepresentation::Hash128})
    for (const bool attach_arrival_numbers : {false, true})
    {
        const GroupLimitSpillLayout layout(header, {0}, representation, attach_arrival_numbers, /*group_limit_end_=*/ 1);
        for (const size_t rows : {0, 1, 256, 65536})
        {
            SCOPED_TRACE(::testing::Message() << "representation=" << static_cast<int>(representation)
                << ", attach_arrival_numbers=" << attach_arrival_numbers << ", rows=" << rows);
            Chunk input(Columns{ColumnUInt64::create(rows, UInt64{0})}, rows);
            const auto prepared = layout.prepareInputChunk(std::move(input), 0);
            size_t actual_bytes = 0;
            for (size_t pos = 1; pos < prepared.getNumColumns(); ++pos)
                actual_bytes += prepared.getColumns()[pos]->allocatedBytes();
            EXPECT_EQ(GroupLimitSpillLayout::estimateServiceColumnsMemory(rows, representation, attach_arrival_numbers), actual_bytes);
        }
    }
}
