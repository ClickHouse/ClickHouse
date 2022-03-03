#include <gtest/gtest.h>

#include <vector>

#include <Columns/ColumnUnique.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>


using namespace DB;

IColumn::Permutation stableGetColumnPermutation(const IColumn & column, IColumn::PermutationSortDirection direction, size_t limit = 0, int nan_direction_hint = 0)
{
    (void)(limit);

    IColumn::Permutation out_permutation;

    size_t size = column.size();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i)
        out_permutation[i] = i;

    std::stable_sort(out_permutation.begin(), out_permutation.end(), [&](size_t lhs, size_t rhs) {
        int res = column.compareAt(lhs, rhs, column, nan_direction_hint);

        if (direction == IColumn::PermutationSortDirection::Ascending)
            return res < 0;
        else
            return res > 0;
    });

    return out_permutation;
}

IColumn::Permutation columnGetPermutation(const IColumn & column, IColumn::PermutationSortDirection direction, size_t limit = 0, int nan_direction_hint = 0)
{
    IColumn::Permutation column_permutation;
    column.getPermutation(direction, IColumn::PermutationSortStability::Stable, limit, nan_direction_hint, column_permutation);

    return column_permutation;
}

void printColumn(const IColumn & column)
{
    size_t column_size = column.size();
    Field value;

    for (size_t i = 0; i < column_size; ++i) {
        column.get(i, value);
        std::cout << value.dump() << ' ';
    }

    std::cout << std::endl;
}

template <typename T>
struct IndexInRangeValueTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        return Field(static_cast<T>(range_index * index_in_range));
    }
};

template <typename ValueTransform>
void generateRanges(std::vector<std::vector<Field>> & ranges, size_t range_size, ValueTransform value_transform)
{
    for (auto & range : ranges) {
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
    for (const auto & range_permutation : ranges_permutations) {
        auto & range = ranges[range_permutation];

        for (auto & value : range) {
            column.insert(value);
        }
    }
}

void assertPermutationsWithLimit(const IColumn::Permutation & lhs, const IColumn::Permutation & rhs, size_t limit) {
    ASSERT_EQ(lhs.size(), rhs.size());

    if (limit == 0) {
        limit = lhs.size();
    }

    for (size_t i = 0; i < limit; ++i) {
        ASSERT_EQ(lhs[i], rhs[i]);
    }
}

void assertColumnPermutation(const IColumn & column, IColumn::PermutationSortDirection direction, size_t limit = 0, int nan_direction_hint = 0)
{
    // std::cout << "Limit " << limit << std::endl;

    auto expected = stableGetColumnPermutation(column, direction, limit, nan_direction_hint);
    auto actual = columnGetPermutation(column, direction, limit, nan_direction_hint);

    if (limit == 0) {
        limit = actual.size();
    }

    // std::cout << "Column" << std::endl;
    // printColumn(column);

    // std::cout << "Expected " << std::endl;
    // for (size_t i = 0; i < limit; ++i) {
    //     std::cout << expected[i] << ' ';
    // }
    // std::cout << std::endl;

    // std::cout << "Actual " << std::endl;
    // for (size_t i = 0; i < limit; ++i) {
    //     std::cout << actual[i] << ' ';
    // }
    // std::cout << std::endl;

    assertPermutationsWithLimit(actual, expected, limit);
}

template <typename ColumnCreateFunc, typename ValueTransform>
void assertColumnPermutations(ColumnCreateFunc column_create_func, ValueTransform value_transform)
{
    static constexpr size_t ranges_size = 3;
    static const std::vector<size_t> range_sizes = { 5, 50, 500, 5000 };

    std::vector<std::vector<Field>> ranges(ranges_size);
    std::vector<size_t> ranges_permutations(ranges_size);
    for (size_t i = 0; i < ranges_size; ++i) {
        ranges_permutations[i] = i;
    }

    for (const auto & range_size : range_sizes) {
        generateRanges(ranges, range_size, value_transform);
        std::sort(ranges_permutations.begin(), ranges_permutations.end());

        while (true)
        {
            auto column_ptr = column_create_func();
            auto & column = *column_ptr;
            insertRangesIntoColumn(ranges, ranges_permutations, column);

            // printColumn(column);

            static constexpr size_t limit_parts = 4;

            size_t column_size = column.size();
            size_t column_limit_part = (column_size / limit_parts) + 1;

            for (size_t limit = 0; limit < column_size; limit += column_limit_part)
            {
                assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, limit, -1);
                assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, limit, 1);

                assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, limit, -1);
                assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, limit, 1);
            }

            assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, 0, -1);
            assertColumnPermutation(column, IColumn::PermutationSortDirection::Ascending, 0, 1);

            assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, 0, -1);
            assertColumnPermutation(column, IColumn::PermutationSortDirection::Descending, 0, 1);

            if (!std::next_permutation(ranges_permutations.begin(), ranges_permutations.end()))
                break;
        }
    }
}

TEST(StablePermutation, ColumnVectorInteger)
{
    auto create_column = []() {
        return ColumnVector<Int64>::create();
    };

    assertColumnPermutations(create_column, IndexInRangeValueTransform<Int64>());
}

struct IndexInRangeFloat64Transform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        if (range_index % 2 == 0 && index_in_range % 4 == 0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        } else if (range_index % 2 == 0 && index_in_range % 5 == 0) {
            return -std::numeric_limits<Float64>::infinity();
        } else if (range_index % 2 == 0 && index_in_range % 6 == 0) {
            return std::numeric_limits<Float64>::infinity();
        }

        return Field(static_cast<Float64>(range_index * index_in_range));
    }
};

TEST(StablePermutation, ColumnVectorFloat)
{
    auto create_column = []() {
        return ColumnVector<Float64>::create();
    };

    assertColumnPermutations(create_column, IndexInRangeFloat64Transform());
}

struct IndexInRangeStringTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        return Field(std::to_string(range_index * index_in_range));
    }
};

TEST(StablePermutation, ColumnString)
{
    auto create_column = []() {
        return ColumnString::create();
    };

    assertColumnPermutations(create_column, IndexInRangeStringTransform());
}

TEST(StablePermutation, ColumnFixedString)
{
    auto create_column = []() {
        return ColumnFixedString::create(15);
    };

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

    auto create_column = [&]() {
        return array_data_type->createColumn();
    };

    assertColumnPermutations(create_column, IndexInRangeArrayTransform());
}

template <typename InnerTransform>
struct IndexInRangeNullableTransform
{
    Field operator()(size_t range_index, size_t index_in_range) const
    {
        if (range_index % 2 == 0 && index_in_range % 3 == 0) {
            return Field();
        }

        return transform(range_index, index_in_range);
    }

    InnerTransform transform;
};

struct IndexInRangeToNullTransform
{
    Field operator()(size_t /*range_index*/, size_t /*index_in_range*/) const
    {
        return Field();
    }
};


TEST(StablePermutation, ColumnNullable)
{
    {
        auto int_data_type = std::make_shared<DataTypeInt64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(int_data_type));

        auto create_column = [&]() {
            return nullable_data_type->createColumn();
        };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeValueTransform<Int64>>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeValueTransform<Int64>());
    }
    {
        auto float_data_type = std::make_shared<DataTypeFloat64>();
        auto nullable_data_type = std::make_shared<DataTypeNullable>(std::move(float_data_type));

        auto create_column = [&]() {
            return nullable_data_type->createColumn();
        };

        assertColumnPermutations(create_column, IndexInRangeNullableTransform<IndexInRangeFloat64Transform>());
        assertColumnPermutations(create_column, IndexInRangeToNullTransform());
        assertColumnPermutations(create_column, IndexInRangeValueTransform<Float64>());
    }
}
