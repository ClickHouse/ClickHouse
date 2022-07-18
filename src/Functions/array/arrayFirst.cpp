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

enum class ArrayFirstLastStrategy
{
    First,
    Last
};

template <ArrayFirstLastStrategy strategy>
struct ArrayFirstLastImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return array_element;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                const auto & data = array.getData();
                auto out = data.cloneEmpty();
                out->reserve(data.size());

                size_t offsets_size = offsets.size();
                for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
                {
                    size_t start_offset = offsets[offset_index - 1];
                    size_t end_offset = offsets[offset_index];

                    if (end_offset > start_offset)
                    {
                        if constexpr (strategy == ArrayFirstLastStrategy::First)
                            out->insert(data[start_offset]);
                        else
                            out->insert(data[end_offset - 1]);
                    }
                    else
                    {
                        out->insertDefault();
                    }
                }

                return out;
            }
            else
            {
                auto out = array.getData().cloneEmpty();
                out->insertDefault();
                return out->replicate(IColumn::Offsets(1, array.size()));
            }
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        const auto & data = array.getData();
        auto out = data.cloneEmpty();
        out->reserve(data.size());

        size_t offsets_size = offsets.size();
        for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
        {
            size_t start_offset = offsets[offset_index - 1];
            size_t end_offset = offsets[offset_index];

            bool exists = false;

            if constexpr (strategy == ArrayFirstLastStrategy::First)
            {
                for (; start_offset != end_offset; ++start_offset)
                {
                    if (filter[start_offset])
                    {
                        out->insert(data[start_offset]);
                        exists = true;
                        break;
                    }
                }
            }
            else
            {
                for (; end_offset != start_offset; --end_offset)
                {
                    if (filter[end_offset - 1])
                    {
                        out->insert(data[end_offset - 1]);
                        exists = true;
                        break;
                    }
                }
            }

            if (!exists)
                out->insertDefault();
        }

        return out;
    }
};

struct NameArrayFirst { static constexpr auto name = "arrayFirst"; };
using ArrayFirstImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::First>;
using FunctionArrayFirst = FunctionArrayMapped<ArrayFirstImpl, NameArrayFirst>;

struct NameArrayLast { static constexpr auto name = "arrayLast"; };
using ArrayLastImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::Last>;
using FunctionArrayLast = FunctionArrayMapped<ArrayLastImpl, NameArrayLast>;

void registerFunctionArrayFirst(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFirst>();
    factory.registerFunction<FunctionArrayLast>();
}

}

