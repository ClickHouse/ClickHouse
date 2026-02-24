#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionCurrentSchemas : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentSchemas";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentSchemas>(context->getCurrentDatabase());
    }

    explicit FunctionCurrentSchemas(const String & db_name_) :
        db_name{db_name_}
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        // For compatibility, function implements the same signature as Postgres'
        const bool argument_is_valid = arguments.size() == 1 && isBool(arguments.front());
        if (!argument_is_valid)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be bool", getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeArray(std::make_shared<DataTypeString>())
                               .createColumnConst(input_rows_count, Array { db_name });
    }
};

}

REGISTER_FUNCTION(CurrentSchema)
{
    FunctionDocumentation::Description description = R"(
Same as function [`currentDatabase`](#currentDatabase) but
- accepts a boolean argument which is ignored
- returns the database name as an array with a single value.

Function `currentSchemas` only exists for compatibility with PostgreSQL.
Please use `currentDatabase` instead.

Also see the [`SET` statement](/sql-reference/statements/use).
    )";
    FunctionDocumentation::Syntax syntax = "currentSchemas(bool)";
    FunctionDocumentation::Arguments arguments = {
        {"bool", "A boolean value, which is ignored.", {"Bool"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a single-element array with the name of the current database.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT currentSchemas(true)
        )",
        R"(
┌─currentSchemas(true)─┐
│ ['default']          │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentSchemas>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_schemas", FunctionCurrentSchemas::name, FunctionFactory::Case::Insensitive);
}

}
