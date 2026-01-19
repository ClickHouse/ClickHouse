#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionGetServerSetting : public IFunction, WithContext
{
public:
    static constexpr auto name = "getServerSetting";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetServerSetting>(context_); }
    explicit FunctionGetServerSetting(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1 ; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto value = getValue(arguments);
        return applyVisitor(FieldToDataType{}, value);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto value = getValue(arguments);
        return result_type->createColumnConst(input_rows_count, convertFieldToType(value, *result_type));
    }

private:
    Field getValue(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Number of arguments for function {} can't be {}, should be 1",
                getName(),
                arguments.size());

        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument of function {} should be a constant string with the name of a setting",
                            String{name});
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The argument of function {} should be a constant string with the name of a setting",
                            String{name});

        std::string_view setting_name{column->getDataAt(0).toView()};

        return getContext()->getServerSettings().get(setting_name);
    }
};

}

REGISTER_FUNCTION(GetServerSetting)
{
    FunctionDocumentation::Description description = R"(
Returns the currently set value, given a server setting name.
    )";
    FunctionDocumentation::Syntax syntax = "getServerSetting(setting_name')";
    FunctionDocumentation::Arguments arguments = {
        {"setting_name", "The server setting name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the server setting's current value.",{"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT getServerSetting('allow_use_jemalloc_memory');
        )",
        R"(
┌─getServerSetting('allow_use_jemalloc_memory')─┐
│ true                                          │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetServerSetting>(documentation, FunctionFactory::Case::Sensitive);
}

}
