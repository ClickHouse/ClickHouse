#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <bool reverse>
struct ArraySplitImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(array_element)
        );
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_cut = typeid_cast<const ColumnUInt8 *>(&*mapped);

        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_offsets_2 = ColumnArray::ColumnOffsets::create();
        auto column_offsets_1 = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & out_offsets_2 = column_offsets_2->getData();
        IColumn::Offsets & out_offsets_1 = column_offsets_1->getData();

        if (column_cut)
        {
            const IColumn::Filter & cut = column_cut->getData();

            size_t pos = 0;

            out_offsets_2.reserve(in_offsets.size()); // assume the actual size to be equal or larger
            out_offsets_1.reserve(in_offsets.size());

            for (auto in_offset : in_offsets)
            {
                if (pos < in_offset)
                {
                    pos += !reverse;
                    for (; pos < in_offset - reverse; ++pos)
                        if (cut[pos])
                            out_offsets_2.push_back(pos + reverse);
                    pos += reverse;

                    out_offsets_2.push_back(pos);
                }

                out_offsets_1.push_back(out_offsets_2.size());
            }
        }
        else
        {
            const auto * column_cut_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_cut_const)
                throw Exception("Unexpected type of cut column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_cut_const->getValue<UInt8>())
            {
                out_offsets_2.reserve(in_offsets.back());
                out_offsets_1.reserve(in_offsets.size());

                for (size_t i = 0; i < in_offsets.back(); ++i)
                    out_offsets_2.push_back(i + 1);
                for (auto in_offset : in_offsets)
                    out_offsets_1.push_back(in_offset);
            }
            else
            {
                size_t pos = 0;

                out_offsets_2.reserve(in_offsets.size());
                out_offsets_1.reserve(in_offsets.size());

                for (auto in_offset : in_offsets)
                {
                    if (pos < in_offset)
                    {
                        pos = in_offset;
                        out_offsets_2.push_back(pos);
                    }

                    out_offsets_1.push_back(out_offsets_2.size());
                }
            }
        }

        return ColumnArray::create(
            ColumnArray::create(
                array.getDataPtr(),
                std::move(column_offsets_2)
            ),
            std::move(column_offsets_1)
        );
    }
};

struct NameArraySplit { static constexpr auto name = "arraySplit"; };
struct NameArrayReverseSplit { static constexpr auto name = "arrayReverseSplit"; };
using FunctionArraySplit = FunctionArrayMapped<ArraySplitImpl<false>, NameArraySplit>;
using FunctionArrayReverseSplit = FunctionArrayMapped<ArraySplitImpl<true>, NameArrayReverseSplit>;

void registerFunctionsArraySplit(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySplit>();
    factory.registerFunction<FunctionArrayReverseSplit>();
}

}
