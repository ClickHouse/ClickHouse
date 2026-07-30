#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionCurrentQueryID : public IFunction
{
public:
    static constexpr auto name = "currentQueryID";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentQueryID>(context->getCurrentQueryId());
    }

    explicit FunctionCurrentQueryID(const String & query_id_) : query_id{query_id_} {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, query_id);
    }

private:
    const String query_id;
};

}

REGISTER_FUNCTION(CurrentQueryID)
{
    factory.registerFunction<FunctionCurrentQueryID>(FunctionDocumentation{
        .description = R"(
Returns current Query id.
)",
        .syntax = {"currentQueryID()"},
        .examples = {{{
            "Example",
            R"(
SELECT currentQueryID();
)",
            R"(
┌─currentQueryID()─────────────────────┐
│ 1280d0e8-1a08-4524-be6e-77975bb68e7d │
└──────────────────────────────────────┘
)"}}},
        .introduced_in = {25, 2},
        .category = FunctionDocumentation::Category::Other,
    });
    factory.registerAlias("current_query_id", FunctionCurrentQueryID::name, FunctionFactory::Case::Insensitive);
}

}
