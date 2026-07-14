#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/arraySort.h>
#include <Functions/castTypeToEither.h>
#include <Common/iota.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

        /// Nulls always go to the end, independent of the sort direction.
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

/// Reads limit[row] from the limit column and rejects a negative value for a signed column.
size_t readLimit(const IColumn & limit_column, bool limit_is_signed, size_t row, const char * function_name)
{
    const UInt64 limit = limit_column.getUInt(row);
    /// For a signed limit column a negative value is reinterpreted as a huge UInt64 with the high bit set;
    /// detect that and throw, like arrayTopK does for its K argument.
    if (limit_is_signed && limit > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument limit of function {} must be non-negative, got {}",
            function_name,
            static_cast<Int64>(limit));
    return limit;
}

/// Builds and returns a permutation that sorts (or partially sorts, when `is_partial`) every array
/// row using `cmp`. The limit (how many elements to partially sort) may differ from row to row when
/// it is passed as a non-constant column, so it is read per-row inside the loop.
template <bool is_partial, typename Comparator>
IColumn::Permutation applyComparator(
    const Comparator & cmp,
    const IColumn * limit_column,
    bool limit_is_signed,
    const ColumnArray::Offsets & offsets,
    const char * function_name)
{
    const size_t nested_size = offsets.empty() ? 0 : offsets.back();
    IColumn::Permutation permutation(nested_size);
    iota(permutation.data(), nested_size, IColumn::Permutation::value_type(0));

    const size_t size = offsets.size();
    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const auto next_offset = offsets[i];
        if constexpr (is_partial)
        {
            const size_t limit = readLimit(*limit_column, limit_is_signed, i, function_name);
            /// With limit == 0 there is nothing to sort, the row keeps its original order.
            if (limit)
            {
                const auto effective_limit = std::min<size_t>(limit, next_offset - current_offset);
                ::partial_sort(&permutation[current_offset], &permutation[current_offset + effective_limit], &permutation[next_offset], cmp);
            }
        }
        else
        {
            ::sort(&permutation[current_offset], &permutation[next_offset], cmp);
        }
        current_offset = next_offset;
    }

    return permutation;
}

}

template <bool positive, bool is_partial>
ColumnPtr ArraySortImpl<positive, is_partial>::execute(
    const ColumnArray & array,
    ColumnPtr mapped,
    const ColumnWithTypeAndName * fixed_arguments)
{
    const IColumn * limit_column = nullptr;
    bool limit_is_signed = false;
    const char * function_name = positive ? "arrayPartialSort" : "arrayPartialReverseSort";
    if constexpr (is_partial)
    {
        if (!fixed_arguments)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected fixed arguments to get the limit for partial array sort");

        limit_column = fixed_arguments[0].column.get();
        limit_is_signed = isNativeInt(*fixed_arguments[0].type);
    }

    const ColumnArray::Offsets & offsets = array.getOffsets();

    /// Peel `Nullable` so the specialized comparator matches the underlying type. A non-nullable column
    /// uses `Less<T>`; a nullable column uses `NullableLess<T>`, which sorts nulls to the end.
    const IColumn * mapped_column = mapped.get();
    const auto * nullable = checkAndGetColumn<ColumnNullable>(mapped_column);
    const IColumn & dispatch_column = nullable ? nullable->getNestedColumn() : *mapped_column;
    const NullMap * null_map = nullable ? &nullable->getNullMapData() : nullptr;

    /// Try a comparator specialized on the concrete column type so `compareAt` is devirtualized.
    IColumn::Permutation permutation;
    bool dispatched = castTypeToEither<
        ColumnUInt8,
        ColumnUInt16,
        ColumnUInt32,
        ColumnUInt64,
        ColumnInt8,
        ColumnInt16,
        ColumnInt32,
        ColumnInt64,
        ColumnFloat32,
        ColumnFloat64,
        ColumnDateTime64,
        ColumnDecimal<Decimal32>,
        ColumnDecimal<Decimal64>,
        ColumnDecimal<Decimal128>,
        ColumnDecimal<Decimal256>,
        ColumnString,
        ColumnFixedString>(
        &dispatch_column,
        [&](const auto & column)
        {
            using ColumnT = std::decay_t<decltype(column)>;
            if (null_map)
            {
                NullableLess<positive, ColumnT> cmp(column, *null_map);
                permutation = applyComparator<is_partial>(cmp, limit_column, limit_is_signed, offsets, function_name);
            }
            else
            {
                Less<positive, ColumnT> cmp(column);
                permutation = applyComparator<is_partial>(cmp, limit_column, limit_is_signed, offsets, function_name);
            }
            return true;
        });

    /// Fall back to the generic comparator for column types not covered above.
    if (!dispatched)
    {
        GenericLess<positive> cmp(*mapped);
        permutation = applyComparator<is_partial>(cmp, limit_column, limit_is_signed, offsets, function_name);
    }

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
        {"arr1, ..., arrN", "Optional. N additional arrays, in the case when `f` accepts multiple arguments."}
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
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

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
    syntax = "arrayReverseSort([f,] arr [, arr1, ... ,arrN])";
    returned_value = {R"(
Returns the array `x` sorted in descending order if no lambda function is provided, otherwise
it returns an array sorted according to the logic of the provided lambda function, and then reversed. [`Array(T)`](/sql-reference/data-types/array).
    )"};
    examples = {
        {"Example 1", "SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;", "[5,3,4]"},
        {"Example 2", "SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;", "[4,3,5]"},
    };
    documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayReverseSort>(documentation);

    description = R"(
This function is the same as `arraySort` but with an additional `limit` argument allowing partial sorting.

If `limit` is `0`, the source array is returned unchanged (no sorting is performed).
A negative `limit` raises an exception.

:::tip
To retain only the sorted elements use `arrayResize`.
:::
    )";
    syntax = "arrayPartialSort([f,] limit, arr [, arr1, ... ,arrN])";
    arguments = {
        {"f(arr[, arr1, ... ,arrN])", "The lambda function to apply to elements of array `x`.", {"Lambda function"}},
        {"limit", "Number of elements to sort. A `limit` of `0` leaves the array unchanged, a negative `limit` is not allowed.", {"(U)Int*"}},
        {"arr", "Array to be sorted.", {"Array(T)"}},
        {"arr1, ... ,arrN", "N additional arrays, in the case when `f` accepts multiple arguments.", {"Array(T)"}}
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
    documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPartialSort>(documentation);

    description = R"(
This function is the same as `arrayReverseSort` but with an additional `limit` argument allowing partial sorting.

If `limit` is `0`, the source array is returned unchanged (no sorting is performed).
A negative `limit` raises an exception.

:::tip
To retain only the sorted elements use `arrayResize`.
:::
    )";
    syntax = "arrayPartialReverseSort([f,] limit, arr [, arr1, ... ,arrN])";
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
    documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPartialReverseSort>(documentation);
}

}
