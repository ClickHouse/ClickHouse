#include <gtest/gtest.h>

#include <vector>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>


using namespace DB;

void stableGetColumnPermutation(
    const IColumn & column,
    IColumn::PermutationSortDirection direction,
    size_t limit,
    int nan_direction_hint,
    IColumn::Permutation & out_permutation)
{
    (void)(limit);

    size_t size = column.size();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i)
        out_permutation[i] = i;

    std::stable_sort(
        out_permutation.begin(),
        out_permutation.end(),
        [&](size_t lhs, size_t rhs)
        {
            int res = column.compareAt(lhs, rhs, column, nan_direction_hint);

            if (direction == IColumn::PermutationSortDirection::Ascending)
                return res < 0;
            else
                return res > 0;
        });
}

void columnGetPermutation(
    const IColumn & column,
    IColumn::PermutationSortDirection direction,
    size_t limit,
    int nan_direction_hint,
    IColumn::Permutation & out_permutation)
{
    column.getPermutation(direction, IColumn::PermutationSortStability::Stable, limit, nan_direction_hint, out_permutation);
}

void printColumn(const IColumn & column)
{
    size_t column_size = column.size();
    Field value;

    for (size_t i = 0; i < column_size; ++i)
    {
        column.get(i, value);
        std::cout << value.dump() << ' ';
    }

    std::cout << std::endl;
}

template <typename ValueTransform>
void generateRanges(std::vector<std::vector<Field>> & ranges, size_t range_size, ValueTransform value_transform)
{
    for (auto & range : ranges)
    {
        range.clear();
    }

    size_t ranges_size = ranges.size();

    for (size_t range_index = 0; range_index < ranges_size; ++range_index)
    {
        for (size_t index_in_range = 0; index_in_range < range_size; ++index_in_range)
        {
            auto value = value_transform(range_index, index_in_range);
            ranges[range_index].emplace_back(value);
        }
    }
}

void insertRangesIntoColumn(std::vector<std::vector<Field>> & ranges, const std::vector<size_t> & ranges_permutations, IColumn & column)
{
    for (const auto & range_permutation : ranges_permutations)
    {
        auto & range = ranges[range_permutation];

        for (auto & value : range)
        {
            column.insert(value);
        }
    }
}

void assertPermutationsWithLimit(const IColumn::Permutation & lhs, const IColumn::Permutation & rhs, size_t limit)
{
    if (limit == 0)
    {
        limit = lhs.size();
    }

    for (size_t i = 0; i < limit; ++i)
    {
        ASSERT_EQ(lhs[i], rhs[i]);
    }
}

void assertColumnPermutation(
    const IColumn & column,
    IColumn::PermutationSortDirection direction,
    size_t limit,
    int nan_direction_hint,
    IColumn::Permutation & actual_permutation,
    IColumn::Permutation & expected_permutation)
{
    stableGetColumnPermutation(column, direction, limit, nan_direction_hint, expected_permutation);
    columnGetPermutation(column, direction, limit, nan_direction_hint, actual_permutation);

    if (limit == 0)
    {
        limit = actual_permutation.size();
    }

    assertPermutationsWithLimit(actual_permutation, expected_permutation, limit);
}

template <typename ColumnCreateFunc, typename ValueTransform>
void assertColumnPermutations(ColumnCreateFunc column_create_func, ValueTransform value_transform)
{
    static constexpr size_t ranges_size = 3;
    static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

    std::vector<std::vector<Field>> ranges(ranges_size);
    std::vector<size_t> ranges_permutations(ranges_size);
    for (size_t i = 0; i < ranges_size; ++i)
    {
        ranges_permutations[i] = i;
    }

    IColumn::Permutation actual_permutation;
    IColumn::Permutation expected_permutation;

    for (const auto & range_size : range_sizes)
    {
        generateRanges(ranges, range_size, value_transform);
        std::sort(ranges_permutations.begin(), ranges_permutations.end());

        while (true)
        {
            auto column_ptr = column_create_func();
            auto & column = *column_ptr;
            insertRangesIntoColumn(ranges, ranges_permutations, column);

            static constexpr size_t limit_parts = 4;

            size_t column_size = column.size();
            size_t column_limit_part = (column_size / limit_parts) + 1;

            for (size_t limit = 0; limit < column_size; limit += column_limit_part)
            {
                assertColumnPermutation(
                    column, IColumn::PermutationSortDirection::Ascending, limit, -1, actual_permutation, expected_permutation);
                assertColumnPermutation(
                    column, IColumn::PermutationSortDirection::Ascending, limit, 1, actual_permutation, expected_permutation);

                assertColumnPermutation(
                    column, IColumn::PermutationSortDirection::Descending, limit, -1, actual_permutation, expected_permutation);
                assertColumnPermutation(
                    column, IColumn::PermutationSortDirection::Descending, limit, 1, actual_permutation, expected_permutation);
            }

            assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, 0, -1, actual_permutation, expected_permutation);
            assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, 0, 1, actual_permutation, expected_permutation);

            assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, 0, -1, actual_permutation, expected_permutation);
            assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, 0, 1, actual_permutation, expected_permutation);

            if (!std::next_permutation(ranges_permutations.begin(), ranges_permutations.end()))
                break;
        }
    }
}

struct IndexInRangeInt64Transform
{
    Field operator()(size_t range_index, size_t index_in_range) const { return Field(static_cast<Int64>(range_index * index_in_range)); }
};

TEST(StablePermutation, ColumnVectorInteger)
{
    auto create_column = []() { return ColumnVector<Int64>::create(); };

    assertColumnPermutations(create_column, IndexInRangeInt64Transform());
}

struct IndexInRangeFloat64Transform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        if (range_index % 2 == 0 && index_in_range % 4 == 0)
        {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        else if (range_index % 2 == 0 && index_in_range % 5 == 0)
        {
            return -std::numeric_limits<Float64>::infinity();
        }
        else if (range_index % 2 == 0 && index_in_range % 6 == 0)
        {
            return std::numeric_limits<Float64>::infinity();
        }

        return Field(static_cast<Float64>(range_index * index_in_range));
    }
};

TEST(StablePermutation, ColumnVectorFloat)
{
    auto create_column = []() { return ColumnVector<Float64>::create(); };

    assertColumnPermutations(create_column, IndexInRangeFloat64Transform());
}

struct IndexInRangeDecimal64Transform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        return Field(static_cast<Decimal64>(range_index * index_in_range));
    }
};

TEST(StablePermutation, ColumnVectorDecimal)
{
    auto create_column = []() { return ColumnDecimal<Decimal64>::create(0, 0); };

    assertColumnPermutations(create_column, IndexInRangeDecimal64Transform());
}


struct IndexInRangeStringTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const { return Field(std::to_string(range_index * index_in_range)); }
};

TEST(StablePermutation, ColumnString)
{
    auto create_column = []() { return ColumnString::create(); };

    assertColumnPermutations(create_column, IndexInRangeStringTransform());
}

TEST(StablePermutation, ColumnFixedString)
{
    auto create_column = []() { return ColumnFixedString::create(15); };

    assertColumnPermutations(create_column, IndexInRangeStringTransform());
}

struct IndexInRangeArrayTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        Field value = static_cast<Int64>(range_index * index_in_range);
        return Array{value};
    }
};

TEST(StablePermutation, ColumnArray)
{
    auto int64_data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(std::move(int64_data_type));

    auto create_column = [&]() { return array_data_type->createColumn(); };

    assertColumnPermutations(create_column, IndexInRangeArrayTransform());
}

template <typename InnerTransform>
struct IndexInRangeNullableTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        if (range_index % 2 == 0 && index_in_range % 3 == 0)
        {
            return Field();
        }

        return transform(range_index, index_in_range);
    }

    InnerTransform transform;
};

struct IndexInRangeToNullTransform
{
    Field operator()(size_t /*range_index*/, size_t /*index_in_range*/) const { return Field(); }
};

TEST(StablePermutation, ColumnNullable)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(int_data_type));

        auto create_column = [&]() { return nullable_data_type->createColumn(); };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeInt64Transform>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeInt64Transform());
    }
    {
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(float_data_type));

        auto create_column = [&]() { return nullable_data_type->createColumn(); };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeFloat64Transform>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeFloat64Transform());
    }
}

TEST(StablePermutation, ColumnLowCardinality)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto low_cardinality_data_type = std::make_shared<DataTypeLowCardinality>(std::move(int_data_type));

        auto create_column = [&]() { return low_cardinality_data_type->createColumn(); };

        assertColumnPermutations(create_column, IndexInRangeInt64Transform());
    }
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(int_data_type));
        auto low_cardinality_data_type = std::make_shared<DataTypeLowCardinality>(nullable_data_type);

        auto create_column = [&]() { return low_cardinality_data_type->createColumn(); };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeInt64Transform>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeInt64Transform());
    }
    {
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(float_data_type));
        auto low_cardinality_data_type = std::make_shared<DataTypeLowCardinality>(nullable_data_type);

        auto create_column = [&]() { return low_cardinality_data_type->createColumn(); };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeFloat64Transform>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeFloat64Transform());
    }
}

template <typename FirstValueTransform, typename SecondValueTransform>
struct TupleTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        Field first_value = first_value_transform(range_index, index_in_range);
        Field second_value = second_value_transform(range_index, index_in_range);

        return Tuple{std::move(first_value), std::move(second_value)};
    }

    FirstValueTransform first_value_transform;
    SecondValueTransform second_value_transform;
};

TEST(StablePermutation, ColumnTuple)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        DataTypes tuple_data_types = {int_data_type, float_data_type};
        auto tuple_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        auto create_column = [&]() { return tuple_type->createColumn(); };

        assertColumnPermutations(create_column, TupleTransform<IndexInRangeInt64Transform, IndexInRangeFloat64Transform>());
    }
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto float_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(int_data_type);
        DataTypes tuple_data_types = {nullable_data_type, float_type};
        auto tuple_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        auto create_column = [&]() { return tuple_type->createColumn(); };

        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeInt64Transform>, IndexInRangeFloat64Transform>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeToNullTransform>, IndexInRangeFloat64Transform>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeInt64Transform>, IndexInRangeFloat64Transform>());
    }
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto float_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(int_data_type);
        DataTypes tuple_data_types = {float_type, nullable_data_type};
        auto tuple_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        auto create_column = [&]() { return tuple_type->createColumn(); };

        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeFloat64Transform, IndexInRangeNullableTransform<IndexInRangeInt64Transform>>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeFloat64Transform, IndexInRangeNullableTransform<IndexInRangeToNullTransform>>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeFloat64Transform, IndexInRangeNullableTransform<IndexInRangeInt64Transform>>());
    }
    {
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(float_data_type);
        DataTypes tuple_data_types = {nullable_data_type, float_data_type};
        auto tuple_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        auto create_column = [&]() { return tuple_type->createColumn(); };

        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeFloat64Transform>, IndexInRangeFloat64Transform>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeToNullTransform>, IndexInRangeFloat64Transform>());
        assertColumnPermutations(
            create_column, TupleTransform<IndexInRangeNullableTransform<IndexInRangeFloat64Transform>, IndexInRangeFloat64Transform>());
    }
}

template <typename FirstValueTransform, typename SecondValueTransform>
struct MapTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        Field first_value = first_value_transform(range_index, index_in_range);
        Field second_value = second_value_transform(range_index, index_in_range);

        return Map{Tuple{std::move(first_value), std::move(second_value)}};
    }

    FirstValueTransform first_value_transform;
    SecondValueTransform second_value_transform;
};

TEST(StablePermutation, ColumnMap)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        auto map_type = std::make_shared<DataTypeMap>(int_data_type, float_data_type);

        auto create_column = [&]() { return map_type->createColumn(); };

        assertColumnPermutations(create_column, MapTransform<IndexInRangeInt64Transform, IndexInRangeFloat64Transform>());
    }
}

TEST(StablePermutation, ColumnSparse)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();

        auto create_column = [&]() { return ColumnSparse::create(int_data_type->createColumn()); };

        assertColumnPermutations(create_column, IndexInRangeInt64Transform());
    }
    {
        auto float_data_type = std::make_shared<DataTypeFloat64>();

        auto create_column = [&]() { return ColumnSparse::create(float_data_type->createColumn()); };

        assertColumnPermutations(create_column, IndexInRangeFloat64Transform());
    }
}
