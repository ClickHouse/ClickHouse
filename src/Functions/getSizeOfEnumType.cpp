#include <Functions/IFunctionImpl.h>
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        block.getByPosition(result).column = getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments)->cloneResized(input_rows_count);
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments) const override
    {
        if (const auto * type8 = checkAndGetDataType<DataTypeEnum8>(block.getByPosition(arguments[0]).type.get()))
            return DataTypeUInt8().createColumnConst(1, type8->getValues().size());
        else if (const auto * type16 = checkAndGetDataType<DataTypeEnum16>(block.getByPosition(arguments[0]).type.get()))
            return DataTypeUInt16().createColumnConst(1, type16->getValues().size());
        else
            throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


void registerFunctionGetSizeOfEnumType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetSizeOfEnumType>();
}

}
