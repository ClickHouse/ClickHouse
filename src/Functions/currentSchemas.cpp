#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{


namespace
{

class FunctionCurrentSchemas final : public IFunction
{
    const String schema_name;

public:
    static constexpr auto name = "currentSchemas";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentSchemas>(String(context->getCurrentDatabase().getSchemaPart()));
    }

    explicit FunctionCurrentSchemas(const String & schema_name_) :
        schema_name{schema_name_}
    {
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    String getSignatureString() const override
    {
        return "(Bool) -> Array(String)";
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeArray(std::make_shared<DataTypeString>())
                               .createColumnConst(input_rows_count, Array { schema_name });
    }
};

}

REGISTER_FUNCTION(CurrentSchema)
{
    FunctionDocumentation::Description description = R"(
Similar to function [`currentDatabase`](#currentDatabase) but
- accepts a boolean argument which is ignored
- returns the schema name as an array with a single value
- returns an array with empty string if the schema name is not set.

Function `currentSchemas` only exists for compatibility with PostgreSQL.
Please use `currentDatabase` instead.

Also see the [`SET` statement](/reference/statements/use).
    )";
    FunctionDocumentation::Syntax syntax = "currentSchemas(bool)";
    FunctionDocumentation::Arguments arguments = {
        {"bool", "A boolean value, which is ignored.", {"Bool"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a single-element array with the name of the current schema.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT currentSchemas(true)
        )",
        R"(
┌─currentSchemas(true)─┐
│ ['']                 │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentSchemas>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_schemas", FunctionCurrentSchemas::name, FunctionFactory::Case::Insensitive);
}

}
