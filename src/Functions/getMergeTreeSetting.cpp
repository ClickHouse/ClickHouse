#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>
// #include <Core/ServerSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionGetMergeTreeSetting : public IFunction, WithContext
{
public:
    static constexpr auto name = "getMergeTreeSetting";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetMergeTreeSetting>(context_); }
    explicit FunctionGetMergeTreeSetting(ContextPtr context_) : WithContext(context_) {}

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

        return getContext()->getMergeTreeSettings().get(setting_name);
    }
};

}

REGISTER_FUNCTION(GetMergeTreeSetting)
{
    FunctionDocumentation::Description description = R"(
Returns the current value of a MergeTree setting.
)";
    FunctionDocumentation::Syntax syntax = "getMergeTreeSetting(setting_name)";
    FunctionDocumentation::Arguments arguments = {
        {"setting_name", "The setting name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the merge tree setting's current value.", {}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT getMergeTreeSetting('index_granularity');
        )",
        R"(
┌─getMergeTreeSetting('index_granularity')─┐
│                                     8192 │
└──────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetMergeTreeSetting>(documentation);
}

}
