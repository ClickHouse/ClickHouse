#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/assert_cast.h>

#include <numeric>
#include <set>

using namespace DB;

namespace
{

std::multiset<String> collectFixedKeys(const IColumn & column)
{
    EXPECT_TRUE(column.isFixedAndContiguous());
    const size_t key_size = column.sizeOfValueIfFixed();
    const auto data = column.getRawData();
    std::multiset<String> result;
    for (size_t row = 0; row < column.size(); ++row)
        result.emplace(data.data() + row * key_size, key_size);
    return result;
}

void checkExactSpillKeys(const Block & header, Columns input_columns, DistinctKeyRepresentation expected_representation)
{
    const auto input_header = std::make_shared<const Block>(header);
    const size_t num_rows = input_columns.front()->size();
    Chunk input(std::move(input_columns), num_rows);
    DistinctSetFilter filter(*input_header, {}, SizeLimits{});
    filter.prepareForInsert(input);
    ASSERT_EQ(filter.getKeyRepresentation(), expected_representation);

    ColumnNumbers key_positions(header.columns());
    std::iota(key_positions.begin(), key_positions.end(), 0);
    const DistinctSpillLayout layout(input_header, key_positions, expected_representation, false);
    auto ordinary = layout.prepareInputChunk(input.clone(), /*first_arrival_number=*/ 0);
    auto emitted = filter.filter(std::move(input));
    ASSERT_EQ(emitted.getNumRows(), num_rows);
    auto extractor = std::move(filter).extractKeys();
    auto suppression = layout.prepareSuppressionChunk(extractor->next(num_rows, 0));

    const auto & key_name = layout.getKeySortDescription().front().column_name;
    const auto & ordinary_keys =
        *ordinary.getColumns()[layout.getInputRunHeader()->getPositionByName(key_name)];
    const auto & suppression_keys =
        *suppression.getColumns()[layout.getSuppressionRunHeader()->getPositionByName(key_name)];
    EXPECT_EQ(collectFixedKeys(ordinary_keys), collectFixedKeys(suppression_keys));
}

}

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
            const size_t flag_pos = header->getPositionByName(layout.getFlagColumnName());
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
            MergeSorter sorter(header, std::move(runs), layout.getKeySortDescription(), 65536, 0);
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
            auto emitted = filter.filter(std::move(input));
            ASSERT_EQ(emitted.getNumRows(), 2);
            auto extractor = std::move(filter).extractKeys();
            auto suppression = layout.prepareSuppressionChunk(extractor->next(10, 0));
            ASSERT_EQ(suppression.getNumColumns(), 2);
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

TEST(DistinctSpillLayout, AllExactRepresentationsMatchSetKeys)
{
    auto bfloat_zeros = []
    {
        auto column = ColumnBFloat16::create();
        column->insertValue(BFloat16(0.));
        column->insertValue(BFloat16(-0.));
        return column;
    };
    auto float32_zeros = [] { return ColumnFloat32::create(std::initializer_list<Float32>{0., -0.}); };
    auto float64_zeros = [] { return ColumnFloat64::create(std::initializer_list<Float64>{0., -0.}); };
    auto nullable_float64_zeros = [&]
    {
        return ColumnNullable::create(float64_zeros(), ColumnUInt8::create(2, UInt8{0}));
    };

    const auto bfloat16 = std::make_shared<DataTypeBFloat16>();
    const auto float32 = std::make_shared<DataTypeFloat32>();
    const auto float64 = std::make_shared<DataTypeFloat64>();
    const auto uint16 = std::make_shared<DataTypeUInt16>();
    const auto uint32 = std::make_shared<DataTypeUInt32>();
    const auto uint64 = std::make_shared<DataTypeUInt64>();
    const auto uint128 = std::make_shared<DataTypeUInt128>();
    const auto nullable_float64 = std::make_shared<DataTypeNullable>(float64);

    checkExactSpillKeys(
        Block{{bfloat16, "f"}}, Columns{bfloat_zeros()}, DistinctKeyRepresentation::Key16);
    checkExactSpillKeys(
        Block{{float32, "f"}}, Columns{float32_zeros()}, DistinctKeyRepresentation::Key32);
    checkExactSpillKeys(
        Block{{float64, "f"}}, Columns{float64_zeros()}, DistinctKeyRepresentation::Key64);
    checkExactSpillKeys(
        Block{{bfloat16, "f"}, {uint16, "u"}},
        Columns{bfloat_zeros(), ColumnUInt16::create(2, UInt16{7})}, DistinctKeyRepresentation::Keys32);
    checkExactSpillKeys(
        Block{{float32, "f"}, {uint32, "u"}},
        Columns{float32_zeros(), ColumnUInt32::create(2, UInt32{7})}, DistinctKeyRepresentation::Keys64);
    checkExactSpillKeys(
        Block{{float64, "f"}, {uint64, "u"}},
        Columns{float64_zeros(), ColumnUInt64::create(2, UInt64{7})}, DistinctKeyRepresentation::Keys128);
    checkExactSpillKeys(
        Block{{float64, "f"}, {uint128, "u"}},
        Columns{float64_zeros(), ColumnUInt128::create(2, UInt128{7})}, DistinctKeyRepresentation::Keys256);
    checkExactSpillKeys(
        Block{{nullable_float64, "f"}},
        Columns{nullable_float64_zeros()}, DistinctKeyRepresentation::NullableKeys128);
    checkExactSpillKeys(
        Block{{nullable_float64, "f"}, {uint128, "u"}},
        Columns{nullable_float64_zeros(), ColumnUInt128::create(2, UInt128{7})},
        DistinctKeyRepresentation::NullableKeys256);
}

TEST(DistinctSpillLayout, ServiceColumnsMatchActualAllocation)
{
    const auto header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key")});
    for (const auto representation : {
             DistinctKeyRepresentation::Columns, DistinctKeyRepresentation::Hash128, DistinctKeyRepresentation::Key64})
    {
        for (const bool ordered : {false, true})
        {
            DistinctSpillLayout layout(header, {0}, representation, ordered);
            for (const size_t rows : {0, 1, 256, 65536})
            {
                SCOPED_TRACE(::testing::Message() << "representation=" << static_cast<int>(representation)
                    << ", ordered=" << ordered << ", rows=" << rows);
                Chunk input(Columns{ColumnUInt64::create(rows, UInt64{0})}, rows);
                auto prepared = layout.prepareInputChunk(std::move(input), 0);
                size_t actual_bytes = 0;
                /// Exclude the original key and the constant emitted flag; only dense service columns
                /// scale with the input row count.
                for (size_t pos = 1; pos + 1 < prepared.getNumColumns(); ++pos)
                    actual_bytes += prepared.getColumns()[pos]->allocatedBytes();
                EXPECT_EQ(DistinctSpillLayout::estimateServiceColumnsMemory(rows, representation, ordered), actual_bytes);
            }
        }
    }
}
