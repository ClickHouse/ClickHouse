#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Returns number of fields in Enum data type of passed value.
class FunctionGetSizeOfEnumType : public IFunction
{
public:
    static constexpr auto name = "getSizeOfEnumType";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGetSizeOfEnumType>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);

        if (which.isEnum8())
            return std::make_shared<DataTypeUInt8>();
        else if (which.isEnum16())
            return std::make_shared<DataTypeUInt16>();

        throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return getSizeOfEnumType(arguments[0].type, input_rows_count);
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return getSizeOfEnumType(arguments[0].type, 1);
    }

private:

    ColumnPtr getSizeOfEnumType(const DataTypePtr & data_type, size_t input_rows_count) const
    {
        if (const auto * type8 = checkAndGetDataType<DataTypeEnum8>(data_type.get()))
            return DataTypeUInt8().createColumnConst(input_rows_count, type8->getValues().size());
        else if (const auto * type16 = checkAndGetDataType<DataTypeEnum16>(data_type.get()))
            return DataTypeUInt16().createColumnConst(input_rows_count, type16->getValues().size());
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument for function {} must be Enum", getName());
    }
};

}

REGISTER_FUNCTION(GetSizeOfEnumType)
{
    factory.registerFunction<FunctionGetSizeOfEnumType>();
}

}
