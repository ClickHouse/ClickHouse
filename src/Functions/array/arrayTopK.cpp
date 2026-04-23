#include <Functions/array/arrayTopK.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Comparator specialized for a concrete column type so `compareAt` is devirtualized and can be inlined.
template <bool IsAscending, typename ColumnType>
struct Less
{
    const ColumnType & column;

    explicit Less(const IColumn & column_)
        : column(assert_cast<const ColumnType &>(column_))
    {
    }

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (IsAscending)
            return column.compareAt(lhs, rhs, column, 1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

/// Comparator fallback for column types not covered by the specialized dispatch — uses virtual `compareAt`.
template <bool IsAscending>
struct GenericLess
{
    const IColumn & column;

    explicit GenericLess(const IColumn & column_) : column(column_) {}

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (IsAscending)
            return column.compareAt(lhs, rhs, column, 1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

/// Reads K[row] from the K column and clamps negatives (signed columns) to 0.
size_t readK(const IColumn & k_column, bool k_is_signed, size_t row)
{
    const UInt64 k = k_column.getUInt(row);
    /// For a signed K column, a negative value is reinterpreted as a huge UInt64 with the high bit set;
    /// detect that and clamp to 0.
    if (k_is_signed && k > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        return 0;
    return k;
}

/// K-selection pass for a single call, specialized at compile time on the comparator type.
/// Builds the result `ColumnArray`.
template <typename Comparator>
ColumnPtr applyComparator(
    const IColumn & k_column,
    bool k_is_signed,
    const ColumnArray & source,
    const IColumn & mapped)
{
    const auto & offsets = source.getOffsets();
    const size_t size = offsets.size();
    const size_t nested_size = source.getData().size();

    /// Peel `Nullable` from `mapped` so the specialized `Less<T>` matches the underlying type,
    /// and cache the null maps for direct array access in the inner loop.
    const auto * mapped_nullable = checkAndGetColumn<ColumnNullable>(&mapped);
    const IColumn & mapped_data = mapped_nullable ? mapped_nullable->getNestedColumn() : mapped;
    const NullMap * mapped_null_map = mapped_nullable ? &mapped_nullable->getNullMapData() : nullptr;

    /// Same peel for the source data column; the non-null nested column is what we use to build
    /// the result (matches the declared return type).
    const auto * source_nullable = checkAndGetColumn<ColumnNullable>(&source.getData());
    const IColumn & source_data = source_nullable ? source_nullable->getNestedColumn() : source.getData();
    const NullMap * source_null_map = source_nullable ? &source_nullable->getNullMapData() : nullptr;

    auto indexes_column = ColumnUInt64::create();
    auto & indexes = indexes_column->getData();

    /// Smaller reserve when K is constant.
    if (isColumnConst(k_column))
        indexes.reserve(std::min(readK(k_column, k_is_signed, 0) * size, nested_size));
    else
        indexes.reserve(nested_size);

    auto result_offsets_column = ColumnArray::ColumnOffsets::create(size);
    auto & result_offsets = result_offsets_column->getData();

    PaddedPODArray<size_t> row_indices;
    ColumnArray::Offset current_offset = 0;
    ColumnArray::Offset result_offset = 0;

    Comparator cmp(mapped_data);
    for (size_t i = 0; i < size; ++i)
    {
        const auto next_offset = offsets[i];

        const size_t k = readK(k_column, k_is_signed, i);
        if (!k)
        {
            result_offsets[i] = result_offset;
            current_offset = next_offset;
            continue;
        }

        row_indices.clear();
        row_indices.reserve(next_offset - current_offset);
        for (size_t j = current_offset; j < next_offset; ++j)
        {
            if (mapped_null_map && (*mapped_null_map)[j])
                continue;
            if (source_null_map && (*source_null_map)[j])
                continue;
            row_indices.push_back(j);
        }

        const size_t take = std::min(k, row_indices.size());

        if (take == row_indices.size())
            ::sort(row_indices.begin(), row_indices.end(), cmp);
        else
            ::partial_sort(row_indices.begin(), row_indices.begin() + take, row_indices.end(), cmp);

        for (size_t j = 0; j < take; ++j)
            indexes.push_back(row_indices[j]);

        result_offset += take;
        result_offsets[i] = result_offset;
        current_offset = next_offset;
    }

    return ColumnArray::create(source_data.index(*indexes_column, 0), std::move(result_offsets_column));
}

/// Iterates the specialized-column type list, falling back to `GenericLess`.
template <bool IsAscending>
ColumnPtr dispatchByColumn(
    const IColumn & k_column,
    bool k_is_signed,
    const ColumnArray & source,
    const IColumn & mapped)
{
    /// A constant lambda (e.g. `(x) -> NULL`) gives `mapped` as `ColumnConst(Nullable(...))`.
    /// Materialize it so the `Nullable` peel below and the null-map access in `applyComparator` work uniformly.
    auto mapped_full = mapped.convertToFullColumnIfConst();

    /// To match a specialized `Less<T>` we look at the type underneath any `Nullable` wrapper.
    const IColumn * mapped_data = mapped_full.get();
    if (const auto * mapped_nullable = checkAndGetColumn<ColumnNullable>(mapped_full.get()))
        mapped_data = &mapped_nullable->getNestedColumn();

#define DISPATCH(TYPE) \
    if (checkAndGetColumn<TYPE>(mapped_data)) \
    { \
        using LessT = Less<IsAscending, TYPE>; \
        return applyComparator<LessT>(k_column, k_is_signed, source, *mapped_full); \
    }

    DISPATCH(ColumnUInt8)
    DISPATCH(ColumnUInt16)
    DISPATCH(ColumnUInt32)
    DISPATCH(ColumnUInt64)
    DISPATCH(ColumnInt8)
    DISPATCH(ColumnInt16)
    DISPATCH(ColumnInt32)
    DISPATCH(ColumnInt64)
    DISPATCH(ColumnFloat32)
    DISPATCH(ColumnFloat64)
    DISPATCH(ColumnDateTime64)
    DISPATCH(ColumnDecimal<Decimal32>)
    DISPATCH(ColumnDecimal<Decimal64>)
    DISPATCH(ColumnDecimal<Decimal128>)
    DISPATCH(ColumnDecimal<Decimal256>)
    DISPATCH(ColumnString)
    DISPATCH(ColumnFixedString)
#undef DISPATCH

    return applyComparator<GenericLess<IsAscending>>(k_column, k_is_signed, source, *mapped_full);
}

}

template <bool IsAscending>
ColumnPtr ArrayTopKImpl<IsAscending>::execute(
    const ColumnArray & array,
    ColumnPtr mapped,
    const ColumnWithTypeAndName * fixed_arguments)
{
    if (!fixed_arguments)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected fixed arguments to get K for {}",
            IsAscending ? "arrayBottomK" : "arrayTopK");

    const IColumn & k_column = *fixed_arguments[0].column;
    const bool k_is_signed = WhichDataType(fixed_arguments[0].type.get()).isNativeInt();

    return dispatchByColumn<IsAscending>(k_column, k_is_signed, array, *mapped);
}

REGISTER_FUNCTION(ArrayTopK)
{
    FunctionDocumentation::Description description = R"(
Returns an array of the K largest elements of the input array, sorted in descending order.
If a lambda function `f` is specified, elements are compared by the result of `f` applied to each element.
If `f` accepts multiple arguments, additional arrays are passed to `arrayTopK`, their elements
correspond to the arguments of `f`.

`NULL` values are skipped and do not appear in the result. The result size is at most `K`
and may be smaller when the input array contains fewer non-null elements than `K`.
The element type of the result is the non-nullable counterpart of the input element type.

`arrayTopK` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).

See also `arrayBottomK`, which returns the K smallest elements instead.
    )";
    FunctionDocumentation::Syntax syntax = "arrayTopK([f,] K, arr [, arr1, ... ,arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"f(arr[, arr1, ... ,arrN])", "Optional. A lambda function to compute the sort key for each element.", {"Lambda function"}},
        {"K", "The number of largest elements to return.", {"(U)Int*"}},
        {"arr", "An array.", {"Array(T)"}},
        {"arr1, ... ,arrN", "N additional arrays, in the case when `f` accepts multiple arguments.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns up to `K` elements of `arr` with the largest values (or largest lambda results), sorted in descending order.
Nulls are skipped. The returned array has element type `T` even when the input has type `Nullable(T)`.
    )"};
    FunctionDocumentation::Examples examples = {
        {"simple_int", "SELECT arrayTopK(3, [1, 5, 2, 7, 3])", "[7,5,3]"},
        {"skip_nulls", "SELECT arrayTopK(3, [1, NULL, 5, 2, NULL, 7])", "[7,5,2]"},
        {"fewer_than_k", "SELECT arrayTopK(5, [1, NULL, 2])", "[2,1]"},
        {"lambda_simple", "SELECT arrayTopK((x) -> -x, 2, [5, 9, 1, 3])", "[1,3]"},
        {"lambda_multi", "SELECT arrayTopK((x, y) -> y, 2, ['a', 'b', 'c'], [3, 1, 2])", "['a','c']"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayTopK>(documentation);
}

REGISTER_FUNCTION(ArrayBottomK)
{
    FunctionDocumentation::Description description = R"(
Returns an array of the K smallest elements of the input array, sorted in ascending order.
If a lambda function `f` is specified, elements are compared by the result of `f` applied to each element.
If `f` accepts multiple arguments, additional arrays are passed to `arrayBottomK`, their elements
correspond to the arguments of `f`.

`NULL` values are skipped and do not appear in the result. The result size is at most `K`
and may be smaller when the input array contains fewer non-null elements than `K`.
The element type of the result is the non-nullable counterpart of the input element type.

`arrayBottomK` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).

See also:

- `arrayTopK`, which returns the K largest elements instead.
- `arrayPartialSort`, which produces the same K elements at positions `[1..K]` but
also keeps the remaining elements in unspecified order, and does not skip nulls.
    )";
    FunctionDocumentation::Syntax syntax = "arrayBottomK([f,] K, arr [, arr1, ... ,arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"f(arr[, arr1, ... ,arrN])", "Optional. A lambda function to compute the sort key for each element.", {"Lambda function"}},
        {"K", "The number of smallest elements to return.", {"(U)Int*"}},
        {"arr", "An array.", {"Array(T)"}},
        {"arr1, ... ,arrN", "N additional arrays, in the case when `f` accepts multiple arguments.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns up to `K` elements of `arr` with the smallest values (or smallest lambda results), sorted in ascending order.
Nulls are skipped. The returned array has element type `T` even when the input has type `Nullable(T)`.
    )"};
    FunctionDocumentation::Examples examples = {
        {"simple_int", "SELECT arrayBottomK(3, [1, 5, 2, 7, 3])", "[1,2,3]"},
        {"skip_nulls", "SELECT arrayBottomK(3, [1, NULL, 5, 2, NULL, 7])", "[1,2,5]"},
        {"fewer_than_k", "SELECT arrayBottomK(5, [1, NULL, 2])", "[1,2]"},
        {"lambda_simple", "SELECT arrayBottomK((x) -> -x, 2, [5, 9, 1, 3])", "[9,5]"},
        {"lambda_multi", "SELECT arrayBottomK((x, y) -> y, 2, ['a', 'b', 'c'], [3, 1, 2])", "['b','c']"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayBottomK>(documentation);
}

}
