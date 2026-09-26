#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/assert_cast.h>

using namespace DB;

TEST(DistinctSpillLayout, KeepsEmittedFlagConstantThroughSorting)
{
    const auto input_header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    for (const bool preserve_input_order : {false, true})
    {
        const DistinctSpillLayout layout(input_header, {0}, DistinctKeyRepresentation::Columns, preserve_input_order);
        for (const bool suppression : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "preserve_input_order=" << preserve_input_order << ", suppression=" << suppression);
            const auto keys = suppression ? std::vector<UInt64>{3, 1, 2} : std::vector<UInt64>{3, 1, 3, 2, 1};
            auto key_column = ColumnUInt64::create();
            for (const auto key : keys)
                key_column->insertValue(key);
            MutableColumns columns;
            columns.emplace_back(std::move(key_column));
            auto chunk = suppression
                ? layout.prepareSuppressionChunk(std::move(columns))
                : layout.prepareInputChunk(Chunk(std::move(columns), keys.size()), /*first_arrival_number=*/ 0);
            const auto & header = suppression ? layout.getSuppressionRunHeader() : layout.getInputRunHeader();
            const size_t flag_pos = header->getPositionByName(layout.getRunSortDescription()[layout.getKeySortDescription().size()].column_name);
            const ColumnPtr initial_flag = chunk.getColumns()[flag_pos];
            EXPECT_EQ(initial_flag->size(), keys.size());

            Block block = header->cloneWithColumns(chunk.detachColumns());
            if (suppression)
                sortBlock(block, layout.getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
            else
                sortBlockAndDeduplicate(block, layout.getKeySortDescription(), IColumn::PermutationSortStability::Stable);

            EXPECT_EQ(block.rows(), 3);
            const auto & sorted_flag = block.getByPosition(flag_pos).column;
            EXPECT_EQ(sorted_flag->size(), 3);
            for (const auto & flag : {initial_flag, sorted_flag})
            {
                ASSERT_TRUE(isColumnConst(*flag));
                const auto & constant = assert_cast<const ColumnConst &>(*flag);
                EXPECT_EQ(constant.getDataColumn().size(), 1);
                EXPECT_EQ(constant.getUInt(0), suppression);
            }

            Chunks runs;
            runs.emplace_back(block.getColumns(), block.rows());
            MergeSorter sorter(header, std::move(runs), layout.getRunSortDescription(), 65536, 0);
            auto merged = sorter.read();
            const auto & merged_keys = assert_cast<const ColumnUInt64 &>(*merged.getColumns()[0]).getData();
            EXPECT_EQ((std::vector<UInt64>{merged_keys.begin(), merged_keys.end()}), (std::vector<UInt64>{1, 2, 3}));
            const auto & flags = assert_cast<const ColumnUInt8 &>(*merged.getColumns()[flag_pos]).getData();
            ASSERT_EQ(flags.size(), 3);
            for (const auto flag : flags)
                EXPECT_EQ(flag, suppression);
        }
    }
}

TEST(DistinctSpillLayout, SuppressionContainsOnlyRetainedKeys)
{
    constexpr size_t payload_size = 65536;
    for (const bool generic : {false, true})
    {
        for (const bool preserve_input_order : {false, true})
        {
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
            EXPECT_EQ(representation, generic ? DistinctKeyRepresentation::Hash128 : DistinctKeyRepresentation::Columns);
            const DistinctSpillLayout layout(header, {0}, representation, preserve_input_order);
            auto ordinary = layout.prepareInputChunk(input.clone(), 10);
            if (preserve_input_order)
            {
                const auto & arrival_name = layout.getArrivalNumberSortDescription().front().column_name;
                const auto & arrivals = ordinary.getColumns()[layout.getInputRunHeader()->getPositionByName(arrival_name)];
                for (size_t row = 0; row < ordinary.getNumRows(); ++row)
                    EXPECT_EQ(arrivals->getUInt(row), 10 + row);
            }
            EXPECT_EQ(layout.getRunSortDescription().size(), layout.getKeySortDescription().size() + 1 + preserve_input_order);
            EXPECT_EQ(layout.getMergedHeader()->columns(), header->columns() + preserve_input_order);
            EXPECT_EQ(layout.preservesInputOrder(), preserve_input_order);
            Columns merged_columns;
            for (const auto & column : *layout.getMergedHeader())
                merged_columns.push_back(ordinary.getColumns()[layout.getInputRunHeader()->getPositionByName(column.name)]);
            auto restored = layout.restoreOutputChunk(Chunk(std::move(merged_columns), ordinary.getNumRows()));
            EXPECT_EQ(restored.getNumColumns(), header->columns());
            EXPECT_EQ(restored.getNumRows(), ordinary.getNumRows());
            auto emitted = filter.filter(std::move(input));
            ASSERT_EQ(emitted.getNumRows(), 2);
            auto extractor = std::move(filter).extractKeys();
            auto suppression = layout.prepareSuppressionChunk(extractor->next(10, 0));
            ASSERT_EQ(suppression.getNumColumns(), 2 + preserve_input_order);
            ASSERT_EQ(suppression.getNumRows(), 2);
            EXPECT_LT(suppression.allocatedBytes(), payload_size);
            EXPECT_FALSE(layout.getSuppressionRunHeader()->has("payload"));
            const auto & key_name = layout.getKeySortDescription().front().column_name;
            const auto & ordinary_keys = *ordinary.getColumns()[layout.getInputRunHeader()->getPositionByName(key_name)];
            const auto & retained_keys = *suppression.getColumns()[layout.getSuppressionRunHeader()->getPositionByName(key_name)];
            EXPECT_NE(retained_keys.compareAt(0, 1, retained_keys, 1), 0);
            for (size_t row = 0; row < 2; ++row)
                EXPECT_TRUE(retained_keys.compareAt(row, 0, ordinary_keys, 1) == 0
                    || retained_keys.compareAt(row, 1, ordinary_keys, 1) == 0);
        }
    }
}
