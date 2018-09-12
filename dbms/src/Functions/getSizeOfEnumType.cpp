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


/// Returns number of fields in Enum data type of passed value.
class FunctionGetSizeOfEnumType : public IFunction
{
public:
    static constexpr auto name = "getSizeOfEnumType";
    static FunctionPtr create(const Context &)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);

        if (which.isEnum8())
            return std::make_shared<DataTypeUInt8>();
        else if (which.isEnum16())
            return std::make_shared<DataTypeUInt16>();

        throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        if (auto type = checkAndGetDataType<DataTypeEnum8>(block.getByPosition(arguments[0]).type.get()))
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(type->getValues().size()));
        else if (auto type = checkAndGetDataType<DataTypeEnum16>(block.getByPosition(arguments[0]).type.get()))
            block.getByPosition(result).column = DataTypeUInt16().createColumnConst(input_rows_count, UInt64(type->getValues().size()));
        else
            throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


void registerFunctionGetSizeOfEnumType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetSizeOfEnumType>();
}

}
