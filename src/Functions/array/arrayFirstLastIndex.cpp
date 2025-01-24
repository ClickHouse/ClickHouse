#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

enum class ArrayFirstLastIndexStrategy : uint8_t
{
    First,
    Last
};

template <ArrayFirstLastIndexStrategy strategy>
struct ArrayFirstLastIndexImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result of the lambda is expected to be a UInt8", mapped->getDataType());

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                auto & out_index = out_column->getData();

                size_t offsets_size = offsets.size();
                for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
                {
                    size_t start_offset = offsets[offset_index - 1];
                    size_t end_offset = offsets[offset_index];

                    if (end_offset > start_offset)
                    {
                        if constexpr (strategy == ArrayFirstLastIndexStrategy::First)
                            out_index[offset_index] = 1;
                        else
                            out_index[offset_index] = static_cast<UInt32>(end_offset - start_offset);
                    }
                    else
                    {
                        out_index[offset_index] = 0;
                    }
                }

                return out_column;
            }

            return DataTypeUInt32().createColumnConst(array.size(), 0u);
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();

        size_t offsets_size = offsets.size();
        auto out_column = ColumnUInt32::create(offsets_size);
        auto & out_index = out_column->getData();

        for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
        {
            size_t start_offset = offsets[offset_index - 1];
            size_t end_offset = offsets[offset_index];
            size_t result_index = 0;

            if constexpr (strategy == ArrayFirstLastIndexStrategy::First)
            {
                for (size_t index = 1; start_offset != end_offset; ++start_offset, ++index)
                {
                    if (filter[start_offset])
                    {
                        result_index = index;
                        break;
                    }
                }
            }
            else
            {
                for (size_t index = end_offset - start_offset; end_offset != start_offset; --end_offset, --index)
                {
                    if (filter[end_offset - 1])
                    {
                        result_index = index;
                        break;
                    }
                }
            }

            out_index[offset_index] = static_cast<UInt32>(result_index);
        }

        return out_column;
    }
};

struct NameArrayFirstIndex { static constexpr auto name = "arrayFirstIndex"; };
using ArrayFirstIndexImpl = ArrayFirstLastIndexImpl<ArrayFirstLastIndexStrategy::First>;
using FunctionArrayFirstIndex = FunctionArrayMapped<ArrayFirstIndexImpl, NameArrayFirstIndex>;

struct NameArrayLastIndex { static constexpr auto name = "arrayLastIndex"; };
using ArrayLastIndexImpl = ArrayFirstLastIndexImpl<ArrayFirstLastIndexStrategy::Last>;
using FunctionArrayLastIndex = FunctionArrayMapped<ArrayLastIndexImpl, NameArrayLastIndex>;

REGISTER_FUNCTION(ArrayFirstIndex)
{
    factory.registerFunction<FunctionArrayFirstIndex>();
    factory.registerFunction<FunctionArrayLastIndex>();
}

}
