#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <base/getFQDNOrHostName.h>
#include <Core/Field.h>


namespace DB
{

class FunctionFQDN : public IFunction
{
public:
    static constexpr auto name = "FQDN";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionFQDN>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(
            input_rows_count, getFQDNOrHostName())->convertToFullColumnIfConst();
    }
};


REGISTER_FUNCTION(FQDN)
{
    FunctionDocumentation::Description description = R"(
Returns the fully qualified domain name of the ClickHouse server.
    )";
    FunctionDocumentation::Syntax syntax = "FQDN()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the fully qualified domain name of the ClickHouse server.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT fqdn()
        )",
        R"(
┌─FQDN()──────────────────────────┐
│ clickhouse.us-east-2.internal │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFQDN>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("fullHostName", "FQDN");
}

}
