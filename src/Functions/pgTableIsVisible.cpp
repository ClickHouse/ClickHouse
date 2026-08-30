#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace
{

/** pgTableIsVisible(oid) - compatibility function for the PostgreSQL wire protocol,
  * an analog of `pg_catalog.pg_table_is_visible`. PostgreSQL clients (e.g. psql for
  * the `\d` command) use it to filter the tables that are visible in the search path.
  * The emulated `pg_class` view exposes only the tables of the current database,
  * which are all visible, so the function unconditionally returns 1.
  */
class FunctionPgTableIsVisible final : public IFunction
{
public:
    static constexpr auto name = "pgTableIsVisible";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPgTableIsVisible>();
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt8().createColumnConst(input_rows_count, 1u);
    }
};

}

REGISTER_FUNCTION(PgTableIsVisible)
{
    FunctionDocumentation::Description description = R"(
Compatibility function for the PostgreSQL wire protocol, an analog of `pg_catalog.pg_table_is_visible`.
PostgreSQL clients (for example, the `\d` command in `psql`) use it to filter tables visible in the search path.
Since the `pg_class` view emulated by ClickHouse exposes only the tables of the current database, which are all visible, the function unconditionally returns `1`.
    )";
    FunctionDocumentation::Syntax syntax = "pgTableIsVisible(oid)";
    FunctionDocumentation::Arguments arguments = {
        {"oid", "Object identifier of the table, as exposed by the emulated `pg_class` view. The value is ignored.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Always returns `1`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT pg_table_is_visible(0)",
        R"(
┌─pg_table_is_visible(0)─┐
│                      1 │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPgTableIsVisible>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("pg_table_is_visible", FunctionPgTableIsVisible::name, FunctionFactory::Case::Insensitive);
}

}
