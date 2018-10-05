#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct ArrayFirstIndexImpl
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
        auto column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                auto & out_index = out_column->getData();

                size_t pos{};
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_index[i] = offsets[i] - pos > 0;
                    pos = offsets[i];
                }

                return out_column;
            }
            else
                return DataTypeUInt32().createColumnConst(array.size(), UInt64(0));
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        auto out_column = ColumnUInt32::create(offsets.size());
        auto & out_index = out_column->getData();

        size_t pos{};
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt32 index{};
            for (size_t idx{1}; pos < offsets[i]; ++pos, ++idx)
            {
                if (filter[pos])
                {
                    index = idx;
                    pos = offsets[i];
                    break;
                }
            }

            out_index[i] = index;
        }

        return out_column;
    }
};

struct NameArrayFirstIndex  { static constexpr auto name = "arrayFirstIndex"; };
using FunctionArrayFirstIndex = FunctionArrayMapped<ArrayFirstIndexImpl, NameArrayFirstIndex>;

void registerFunctionArrayFirstIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFirstIndex>();
}

}

