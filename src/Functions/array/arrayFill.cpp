#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <bool reverse>
struct ArrayFillImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_fill = typeid_cast<const ColumnUInt8 *>(&*mapped);

        const IColumn & in_data = array.getData();
        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_data = in_data.cloneEmpty();
        IColumn & out_data = *column_data.get();

        if (column_fill)
        {
            const IColumn::Filter & fill = column_fill->getData();

            size_t array_begin = 0;
            size_t array_end = 0;
            size_t begin = 0;
            size_t end = 0;

            out_data.reserve(in_data.size());

            for (auto in_offset : in_offsets)
            {
                array_end = in_offset - 1;

                for (; end <= array_end; ++end)
                {
                    if (end == array_end || fill[end + 1] != fill[begin])
                    {
                        if (fill[begin])
                            out_data.insertRangeFrom(in_data, begin, end + 1 - begin);
                        else
                        {
                            if constexpr (reverse)
                            {
                                if (end == array_end)
                                    out_data.insertManyFrom(in_data, array_end, end + 1 - begin);
                                else
                                    out_data.insertManyFrom(in_data, end + 1, end + 1 - begin);
                            }
                            else
                            {
                                if (begin == array_begin)
                                    out_data.insertManyFrom(in_data, array_begin, end + 1 - begin);
                                else
                                    out_data.insertManyFrom(in_data, begin - 1, end + 1 - begin);
                            }
                        }

                        begin = end + 1;
                    }
                }

                array_begin = array_end + 1;
            }
        }
        else
        {
            const auto * column_fill_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_fill_const)
                throw Exception("Unexpected type of cut column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_fill_const->getValue<UInt8>())
                return ColumnArray::create(
                    array.getDataPtr(),
                    array.getOffsetsPtr()
                );

            size_t array_begin = 0;
            size_t array_end = 0;

            out_data.reserve(in_data.size());

            for (auto in_offset : in_offsets)
            {
                array_end = in_offset - 1;

                if constexpr (reverse)
                    out_data.insertManyFrom(in_data, array_end, array_end + 1 - array_begin);
                else
                    out_data.insertManyFrom(in_data, array_begin, array_end + 1 - array_begin);

                array_begin = array_end + 1;
            }
        }

        return ColumnArray::create(
            std::move(column_data),
            array.getOffsetsPtr()
        );
    }
};

struct NameArrayFill { static constexpr auto name = "arrayFill"; };
struct NameArrayReverseFill { static constexpr auto name = "arrayReverseFill"; };
using FunctionArrayFill = FunctionArrayMapped<ArrayFillImpl<false>, NameArrayFill>;
using FunctionArrayReverseFill = FunctionArrayMapped<ArrayFillImpl<true>, NameArrayReverseFill>;

void registerFunctionsArrayFill(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFill>();
    factory.registerFunction<FunctionArrayReverseFill>();
}

}
