#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{

/** pgGetUserById(oid) - compatibility function for the PostgreSQL wire protocol,
  * an analog of `pg_catalog.pg_get_userbyid`. PostgreSQL clients (e.g. psql for
  * the `\d` command) use it to display the owner of a table. ClickHouse does not
  * track table ownership, so the function returns the name of the current user.
  */
class FunctionPgGetUserById final : public IFunction
{
    const String user_name;

public:
    static constexpr auto name = "pgGetUserById";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionPgGetUserById>(context->getClientInfo().initial_user);
    }

    explicit FunctionPgGetUserById(const String & user_name_) : user_name{user_name_}
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

    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, user_name);
    }
};

}

REGISTER_FUNCTION(PgGetUserById)
{
    FunctionDocumentation::Description description = R"(
Compatibility function for the PostgreSQL wire protocol, an analog of `pg_catalog.pg_get_userbyid`.
PostgreSQL clients (for example, the `\d` command in `psql`) use it to display the owner of a table.
ClickHouse does not track table ownership, so the function ignores the argument and returns the name of the current user.
    )";
    FunctionDocumentation::Syntax syntax = "pgGetUserById(oid)";
    FunctionDocumentation::Arguments arguments = {
        {"oid", "Object identifier of the role. The value is ignored.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the name of the current user.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT pg_get_userbyid(10)",
        R"(
┌─pg_get_userbyid(10)─┐
│ default             │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPgGetUserById>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("pg_get_userbyid", FunctionPgGetUserById::name, FunctionFactory::Case::Insensitive);
}

}
