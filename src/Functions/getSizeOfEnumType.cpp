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
        if (which.isEnum16())
            return std::make_shared<DataTypeUInt16>();

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument for function {} must be Enum", getName());
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
        if (const auto * type16 = checkAndGetDataType<DataTypeEnum16>(data_type.get()))
            return DataTypeUInt16().createColumnConst(input_rows_count, type16->getValues().size());
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument for function {} must be Enum", getName());
    }
};

}

REGISTER_FUNCTION(GetSizeOfEnumType)
{
    FunctionDocumentation::Description description = R"(
Returns the number of fields in the given [`Enum`](../../sql-reference/data-types/enum.md).
)";
    FunctionDocumentation::Syntax syntax = "getSizeOfEnumType(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Value of type `Enum`.", {"Enum"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of fields with `Enum` input values.", {"UInt8/16"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT getSizeOfEnumType(CAST('a' AS Enum8('a' = 1, 'b' = 2))) AS x;
        )",
        R"(
┌─x─┐
│ 2 │
└───┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetSizeOfEnumType>(documentation);
}

}
