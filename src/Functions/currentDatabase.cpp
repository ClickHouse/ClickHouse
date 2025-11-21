#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionCurrentDatabase : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context->getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name_) : db_name{db_name_}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, db_name);
    }
};

}

REGISTER_FUNCTION(CurrentDatabase)
{
    FunctionDocumentation::Description description = R"(
Returns the name of the current database.
Useful in table engine parameters of `CREATE TABLE` queries where you need to specify the database.

Also see the [`SET` statement](/sql-reference/statements/use).
    )";
    FunctionDocumentation::Syntax syntax = "currentDatabase()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current database name.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example", R"(
SELECT currentDatabase()
        )",
        R"(
┌─currentDatabase()─┐
│ default           │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentDatabase>(documentation);
    factory.registerAlias("DATABASE", FunctionCurrentDatabase::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("SCHEMA", FunctionCurrentDatabase::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_database", FunctionCurrentDatabase::name, FunctionFactory::Case::Insensitive);
}

}
