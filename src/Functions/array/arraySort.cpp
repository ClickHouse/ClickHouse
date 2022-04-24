#include "FunctionArrayMapped.h"

#include <base/sort.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

/** Sort arrays, by values of its elements, or by values of corresponding elements of calculated expression (known as "schwartzsort").
  */
template <bool positive>
struct ArraySortImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

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

        explicit Less(const IColumn & column_) : column(column_) {}

        bool operator()(size_t lhs, size_t rhs) const
        {
            if (positive)
                return column.compareAt(lhs, rhs, column, 1) < 0;
            else
                return column.compareAt(lhs, rhs, column, -1) > 0;
        }
    };

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
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
            ::sort(&permutation[current_offset], &permutation[next_offset], Less(*mapped));
            current_offset = next_offset;
        }

        return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
    }
};

struct NameArraySort { static constexpr auto name = "arraySort"; };
struct NameArrayReverseSort { static constexpr auto name = "arrayReverseSort"; };

using FunctionArraySort = FunctionArrayMapped<ArraySortImpl<true>, NameArraySort>;
using FunctionArrayReverseSort = FunctionArrayMapped<ArraySortImpl<false>, NameArrayReverseSort>;

void registerFunctionsArraySort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();
}

}

