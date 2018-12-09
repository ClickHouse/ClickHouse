#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// arrayEnumerate(arr) - Returns the array [1,2,3,..., length(arr)]
class FunctionArrayEnumerate : public IFunction
{
public:
    static constexpr auto name = "arrayEnumerate";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionArrayEnumerate>();
    }

    String getName() const override
    {
        return name;
    }

    String getSignature() const override { return "f(Array) -> Array(UInt32)"; }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        if (const ColumnArray * array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get()))
        {
            const ColumnArray::Offsets & offsets = array->getOffsets();

            auto res_nested = ColumnUInt32::create();

            ColumnUInt32::Container & res_values = res_nested->getData();
            res_values.resize(array->getData().size());
            ColumnArray::Offset prev_off = 0;
            for (ColumnArray::Offset i = 0; i < offsets.size(); ++i)
            {
                ColumnArray::Offset off = offsets[i];
                for (ColumnArray::Offset j = prev_off; j < off; ++j)
                    res_values[j] = j - prev_off + 1;

                prev_off = off;
            }

            block.getByPosition(result).column = ColumnArray::create(std::move(res_nested), array->getOffsetsPtr());
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


void registerFunctionArrayEnumerate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerate>();
}

}
