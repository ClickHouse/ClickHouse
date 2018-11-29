#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct ArrayFirstImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return array_element;
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
                const auto & data = array.getData();
                auto out = data.cloneEmpty();

                size_t pos{};
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    if (offsets[i] - pos > 0)
                        out->insert(data[pos]);
                    else
                        out->insertDefault();

                    pos = offsets[i];
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

        size_t pos{};
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto exists = false;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                {
                    out->insert(data[pos]);
                    exists = true;
                    pos = offsets[i];
                    break;
                }
            }

            if (!exists)
                out->insertDefault();
        }

        return out;
    }
};

struct NameArrayFirst { static constexpr auto name = "arrayFirst"; };
using FunctionArrayFirst = FunctionArrayMapped<ArrayFirstImpl, NameArrayFirst>;

void registerFunctionArrayFirst(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFirst>();
}

}

