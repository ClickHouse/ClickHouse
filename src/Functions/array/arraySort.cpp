#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/arraySort.h>
#include <Common/iota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <bool positive, typename ColumnType>
struct Less
{
    const ColumnType & column;

    explicit Less(const IColumn & column_)
        : column(assert_cast<const ColumnType &>(column_))
    {
    }

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (positive)
            return column.compareAt(lhs, rhs, column, 1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

template <bool positive, typename ColumnType>
struct NullableLess
{
    const ColumnType & nested_column;
    const NullMap & null_map;

    explicit NullableLess(const IColumn & nested_column_, const NullMap & null_map_)
        : nested_column(assert_cast<const ColumnType &>(nested_column_))
        , null_map(null_map_)
    {
    }

    bool operator()(size_t lhs, size_t rhs) const
    {
        bool lhs_is_null = null_map[lhs];
        bool rhs_is_null = null_map[rhs];

        if (lhs_is_null) [[unlikely]]
            return false;
        if (rhs_is_null) [[unlikely]]
            return true;

        if constexpr (positive)
            return nested_column.compareAt(lhs, rhs, nested_column, 1) < 0;
        else
            return nested_column.compareAt(lhs, rhs, nested_column, -1) > 0;
    }
};


template <bool positive>
struct GenericLess
{
    const IColumn & column;

    explicit GenericLess(const IColumn & column_) : column(column_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (positive)
            return column.compareAt(lhs, rhs, column, 1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

}

template <bool positive, bool is_partial>
ColumnPtr ArraySortImpl<positive, is_partial>::execute(
    const ColumnArray & array,
    ColumnPtr mapped,
    const ColumnWithTypeAndName * fixed_arguments)
{
    [[maybe_unused]] const auto limit = [&]() -> size_t
    {
        if constexpr (is_partial)
        {
            if (!fixed_arguments)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Expected fixed arguments to get the limit for partial array sort"
                );

            /// During dryRun the input column might be empty
            if (!fixed_arguments[0].column->empty())
                return fixed_arguments[0].column->getUInt(0);
        }
        return 0;
    }();

    const ColumnArray::Offsets & offsets = array.getOffsets();

    size_t size = offsets.size();
    size_t nested_size = array.getData().size();
    IColumn::Permutation permutation(nested_size);
    iota(permutation.data(), nested_size, IColumn::Permutation::value_type(0));

    ColumnArray::Offset current_offset = 0;

#define APPLY_COMPARATOR(CMP) \
    for (size_t i = 0; i < size; ++i) \
    { \
        auto next_offset = offsets[i]; \
        if constexpr (is_partial) \
        { \
            if (limit) \
            { \
                const auto effective_limit = std::min<size_t>(limit, next_offset - current_offset); \
                ::partial_sort(&permutation[current_offset], &permutation[current_offset + effective_limit], &permutation[next_offset], CMP); \
            } \
            else \
                ::sort(&permutation[current_offset], &permutation[next_offset], CMP); \
        } \
        else \
            ::sort(&permutation[current_offset], &permutation[next_offset], CMP); \
        current_offset = next_offset; \
    }

#define DISPATCH_FOR_NONNULLABLE_COLUMN(TYPE) \
    else if (checkAndGetColumn<TYPE>(mapped.get())) \
    { \
        Less<positive, TYPE> cmp(*mapped); \
        APPLY_COMPARATOR(cmp) \
    }

    if (false)
        ;
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt8)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt16)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt32)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt64)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt8)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt16)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt32)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt64)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFloat32)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFloat64)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDateTime64)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal32>)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal64>)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal128>)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal256>)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnString)
    DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFixedString)
#undef DISPATCH_FOR_NONNULLABLE_COLUMN

    else if (const auto * nullable = checkAndGetColumn<ColumnNullable>(mapped.get()))
    {
        const auto & null_map = nullable->getNullMapData();

#define DISPATCH_FOR_NULLABLE_COLUMN(TYPE) \
    else if (checkAndGetColumn<TYPE>(&nullable->getNestedColumn())) \
    { \
        NullableLess<positive, TYPE> cmp(nullable->getNestedColumn(), null_map); \
        APPLY_COMPARATOR(cmp) \
    }

        if (false)
            ;
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt8)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt16)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt32)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt64)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt8)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt16)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt32)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt64)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnFloat32)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnFloat64)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnDateTime64)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal32>)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal64>)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal128>)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal256>)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnString)
        DISPATCH_FOR_NULLABLE_COLUMN(ColumnFixedString)
        else
        {
            GenericLess<positive> cmp(*mapped);
            APPLY_COMPARATOR(cmp)
        }
#undef DISPATCH_FOR_NULLABLE_COLUMN
    }
    else
    {
        GenericLess<positive> cmp(*mapped);
        APPLY_COMPARATOR(cmp)
    }
#undef APPLY_COMPARATOR

    return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
}

REGISTER_FUNCTION(ArraySort)
{
    FunctionDocumentation::Description description = R"(
Sorts the elements of the provided array in ascending order.
If a lambda function `f` is specified, sorting order is determined by the result of
the lambda applied to each element of the array.
If the lambda accepts multiple arguments, the `arraySort` function is passed several
arrays that the arguments of `f` will correspond to.

If the array to sort contains `-Inf`, `NULL`, `NaN`, or `Inf` they will be sorted in the following order:

1. `-Inf`
2. `Inf`
3. `NaN`
4. `NULL`

`arraySort` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).
)";
    FunctionDocumentation::Syntax syntax = "arraySort([f,] arr [, arr1, ... ,arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"f(y1[, y2 ... yN])", "The lambda function to apply to elements of array `x`."},
        {"arr", "An array to be sorted. [`Array(T)`](/sql-reference/data-types/array)"},
        {"arr1, ..., yN", "Optional. N additional arrays, in the case when `f` accepts multiple arguments."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the array `arr` sorted in ascending order if no lambda function is provided, otherwise
it returns an array sorted according to the logic of the provided lambda function. [`Array(T)`](/sql-reference/data-types/array).
    )"};
    FunctionDocumentation::Examples examples = {
        {"Example 1", "SELECT arraySort([1, 3, 3, 0]);", "[0,1,3,3]"},
        {"Example 2", "SELECT arraySort(['hello', 'world', '!']);", "['!','hello','world']"},
        {"Example 3", "SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);", "[-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArraySort>(documentation);

    description = R"(
Sorts the elements of an array in descending order.
If a function `f` is specified, the provided array is sorted according to the result
of the function applied to the elements of the array, and then the sorted array is reversed.
If `f` accepts multiple arguments, the `arrayReverseSort` function is passed several arrays that
the arguments of `func` will correspond to.

If the array to sort contains `-Inf`, `NULL`, `NaN`, or `Inf` they will be sorted in the following order:

1. `-Inf`
2. `Inf`
3. `NaN`
4. `NULL`

`arrayReverseSort` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).
    )";
    syntax = "arrayReverseSort([f,] arr [, arr1, ... ,arrN)";
    returned_value = {R"(
Returns the array `x` sorted in descending order if no lambda function is provided, otherwise
it returns an array sorted according to the logic of the provided lambda function, and then reversed. [`Array(T)`](/sql-reference/data-types/array).
    )"};
    examples = {
        {"Example 1", "SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;", "[5,3,4]"},
        {"Example 2", "SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;", "[4,3,5]"},
    };
    documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayReverseSort>(documentation);

    description = R"(
This function is the same as `arraySort` but with an additional `limit` argument allowing partial sorting.

:::tip
To retain only the sorted elements use `arrayResize`.
:::
    )";
    syntax = "arrayPartialSort([f,] arr [, arr1, ... ,arrN], limit)";
    arguments = {
        {"f(arr[, arr1, ... ,arrN])", "The lambda function to apply to elements of array `x`.", {"Lambda function"}},
        {"arr", "Array to be sorted.", {"Array(T)"}},
        {"arr1, ... ,arrN", "N additional arrays, in the case when `f` accepts multiple arguments.", {"Array(T)"}},
        {"limit", "Index value up until which sorting will occur.", {"(U)Int*"}}
    };
    returned_value = {R"(
Returns an array of the same size as the original array where elements in the range `[1..limit]` are sorted
in ascending order. The remaining elements `(limit..N]` are in an unspecified order.
    )"};
    examples = {
        {"simple_int", "SELECT arrayPartialSort(2, [5, 9, 1, 3])", "[1, 3, 5, 9]"},
        {"simple_string", "SELECT arrayPartialSort(2, ['expenses', 'lasso', 'embolism', 'gladly'])", "['embolism', 'expenses', 'gladly', 'lasso']"},
        {"retain_sorted", "SELECT arrayResize(arrayPartialSort(2, [5, 9, 1, 3]), 2)", "[1, 3]"},
        {"lambda_simple", "SELECT arrayPartialSort((x) -> -x, 2, [5, 9, 1, 3])", "[9, 5, 1, 3]"},
        {"lambda_complex", "SELECT arrayPartialSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res", "[2, 1, 0]"}
    };
    introduced_in = {23, 2};
    documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPartialSort>(documentation);

    description = R"(
This function is the same as `arrayReverseSort` but with an additional `limit` argument allowing partial sorting.

:::tip
To retain only the sorted elements use `arrayResize`.
:::
    )";
    syntax = "arrayPartialReverseSort([f,] arr [, arr1, ... ,arrN], limit)";
    returned_value = {R"(
Returns an array of the same size as the original array where elements in the range `[1..limit]` are sorted
in descending order. The remaining elements `(limit..N]` are in an unspecified order.
    )"};
    examples = {
        {"simple_int", "SELECT arrayPartialReverseSort(2, [5, 9, 1, 3])", "[9, 5, 1, 3]"},
        {"simple_string", "SELECT arrayPartialReverseSort(2, ['expenses','lasso','embolism','gladly'])", "['lasso','gladly','expenses','embolism']"},
        {"retain_sorted", "SELECT arrayResize(arrayPartialReverseSort(2, [5, 9, 1, 3]), 2)", "[9, 5]"},
        {"lambda_simple", "SELECT arrayPartialReverseSort((x) -> -x, 2, [5, 9, 1, 3])", "[1, 3, 5, 9]"},
        {"lambda_complex", "SELECT arrayPartialReverseSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res", "[0, 1, 2]"}
    };
    documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPartialReverseSort>(documentation);
}

}
