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
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();

    factory.registerFunction<FunctionArrayPartialSort>(FunctionDocumentation{
        .description=R"(
Returns an array of the same size as the original array where elements in range `[1..limit]`
are sorted in ascending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.
[example:simple_int]
[example:simple_string]

To retain only the sorted elements use `arrayResize`:
[example:retain_sorted]

If the `func` function is specified, sorting order is determined by the result of the `func`
function applied to the elements of the array.
[example:lambda_simple]

If `func` accepts multiple arguments, the `arrayPartialSort` function is passed several arrays
that the arguments of `func` will correspond to.
[example:lambda_complex]

For more details see documentation of `arraySort`.
)",
        .examples{
            {"simple_int", "SELECT arrayPartialSort(2, [5, 9, 1, 3])", ""},
            {"simple_string", "SELECT arrayPartialSort(2, ['expenses','lasso','embolism','gladly'])", ""},
            {"retain_sorted", "SELECT arrayResize(arrayPartialSort(2, [5, 9, 1, 3]), 2)", ""},
            {"lambda_simple", "SELECT arrayPartialSort((x) -> -x, 2, [5, 9, 1, 3])", ""},
            {"lambda_complex", "SELECT arrayPartialSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res", ""}},
        .category{"Array"}});

    factory.registerFunction<FunctionArrayPartialReverseSort>(FunctionDocumentation{
        .description=R"(
Returns an array of the same size as the original array where elements in range `[1..limit]`
are sorted in descending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.
[example:simple_int]
[example:simple_string]

To retain only the sorted elements use `arrayResize`:
[example:retain_sorted]

If the `func` function is specified, sorting order is determined by the result of the `func`
function applied to the elements of the array.
[example:lambda_simple]

If `func` accepts multiple arguments, the `arrayPartialSort` function is passed several arrays
that the arguments of `func` will correspond to.
[example:lambda_complex]

For more details see documentation of `arraySort`.
)",
        .examples{
            {"simple_int", "SELECT arrayPartialReverseSort(2, [5, 9, 1, 3])", ""},
            {"simple_string", "SELECT arrayPartialReverseSort(2, ['expenses','lasso','embolism','gladly'])", ""},
            {"retain_sorted", "SELECT arrayResize(arrayPartialReverseSort(2, [5, 9, 1, 3]), 2)", ""},
            {"lambda_simple", "SELECT arrayPartialReverseSort((x) -> -x, 2, [5, 9, 1, 3])", ""},
            {"lambda_complex", "SELECT arrayPartialReverseSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res", ""}},
        .category{"Array"}});
}

}
