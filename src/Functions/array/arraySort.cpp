#include "FunctionArrayMapped.h"

#include <Functions/FunctionFactory.h>
#include <base/sort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

/** Sort arrays, by values of its elements, or by values of corresponding elements of calculated expression (known as "schwartzsort").
  */
template <bool positive, bool is_partial>
struct ArraySortImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static constexpr auto num_fixed_params = is_partial;

    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    struct Less
    {
        const IColumn & column;

        explicit Less(const IColumn & column_) : column(column_) { }

        bool operator()(size_t lhs, size_t rhs) const
        {
            if (positive)
                return column.compareAt(lhs, rhs, column, 1) < 0;
            else
                return column.compareAt(lhs, rhs, column, -1) > 0;
        }
    };

    static void checkArguments(const String & name, const ColumnWithTypeAndName * fixed_arguments)
        requires(num_fixed_params)
    {
        if (!fixed_arguments)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected fixed arguments to get the limit for partial array sort"
            );
        WhichDataType which(fixed_arguments[0].type.get());
        if (!which.isUInt() && !which.isInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                fixed_arguments[0].type->getName(),
                name);
    }

    static ColumnPtr execute(
        const ColumnArray & array,
        ColumnPtr mapped,
        const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]] = nullptr)
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
                return fixed_arguments[0].column.get()->getUInt(0);
            }
            return 0;
        }();

        const ColumnArray::Offsets & offsets = array.getOffsets();

        size_t size = offsets.size();
        size_t nested_size = array.getData().size();
        IColumn::Permutation permutation(nested_size);

        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto next_offset = offsets[i];
            if constexpr (is_partial)
            {
                if (limit)
                {
                    const auto effective_limit = std::min<size_t>(limit, next_offset - current_offset);
                    ::partial_sort(&permutation[current_offset], &permutation[current_offset + effective_limit], &permutation[next_offset], Less(*mapped));
                }
                else
                    ::sort(&permutation[current_offset], &permutation[next_offset], Less(*mapped));
            }
            else
                ::sort(&permutation[current_offset], &permutation[next_offset], Less(*mapped));
            current_offset = next_offset;
        }

        return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
    }
};

struct NameArraySort
{
    static constexpr auto name = "arraySort";
};
struct NameArrayReverseSort
{
    static constexpr auto name = "arrayReverseSort";
};
struct NameArrayPartialSort
{
    static constexpr auto name = "arrayPartialSort";
};
struct NameArrayPartialReverseSort
{
    static constexpr auto name = "arrayPartialReverseSort";
};

using FunctionArraySort = FunctionArrayMapped<ArraySortImpl<true, false>, NameArraySort>;
using FunctionArrayReverseSort = FunctionArrayMapped<ArraySortImpl<false, false>, NameArrayReverseSort>;
using FunctionArrayPartialSort = FunctionArrayMapped<ArraySortImpl<true, true>, NameArrayPartialSort>;
using FunctionArrayPartialReverseSort = FunctionArrayMapped<ArraySortImpl<false, true>, NameArrayPartialReverseSort>;

REGISTER_FUNCTION(ArraySort)
{
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();

    factory.registerFunction<FunctionArrayPartialSort>({
        R"(
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
        Documentation::Examples{
            {"simple_int", "SELECT arrayPartialSort(2, [5, 9, 1, 3])"},
            {"simple_string", "SELECT arrayPartialSort(2, ['expenses','lasso','embolism','gladly'])"},
            {"retain_sorted", "SELECT arrayResize(arrayPartialSort(2, [5, 9, 1, 3]), 2)"},
            {"lambda_simple", "SELECT arrayPartialSort((x) -> -x, 2, [5, 9, 1, 3])"},
            {"lambda_complex", "SELECT arrayPartialSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res"}},
        Documentation::Categories{"Array"}});
    factory.registerFunction<FunctionArrayPartialReverseSort>({
        R"(
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
        Documentation::Examples{
            {"simple_int", "SELECT arrayPartialReverseSort(2, [5, 9, 1, 3])"},
            {"simple_string", "SELECT arrayPartialReverseSort(2, ['expenses','lasso','embolism','gladly'])"},
            {"retain_sorted", "SELECT arrayResize(arrayPartialReverseSort(2, [5, 9, 1, 3]), 2)"},
            {"lambda_simple", "SELECT arrayPartialReverseSort((x) -> -x, 2, [5, 9, 1, 3])"},
            {"lambda_complex", "SELECT arrayPartialReverseSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res"}},
        Documentation::Categories{"Array"}});
}

}
