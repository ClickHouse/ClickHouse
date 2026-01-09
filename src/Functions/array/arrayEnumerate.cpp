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

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayEnumerate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be an array but it has type {}.",
                            getName(), arguments[0]->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        if (const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get()))
        {
            const ColumnArray::Offsets & offsets = array->getOffsets();

            auto res_nested = ColumnUInt32::create();

            ColumnUInt32::Container & res_values = res_nested->getData();
            res_values.resize(array->getData().size());
            ColumnArray::Offset prev_off = 0;
            for (auto off : offsets)
            {
                for (ColumnArray::Offset j = prev_off; j < off; ++j)
                    res_values[j] = static_cast<UInt32>(j - prev_off + 1);
                prev_off = off;
            }

            return ColumnArray::create(std::move(res_nested), array->getOffsetsPtr());
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};


REGISTER_FUNCTION(ArrayEnumerate)
{
    factory.registerFunction<FunctionArrayEnumerate>();
}

}
