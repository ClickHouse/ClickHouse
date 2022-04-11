#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

namespace DB
{
class FunctionInitialQueryID : public IFunction
{
    const String initial_query_id;

public:
    static constexpr auto name = "initialQueryID";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionInitialQueryID>(context->getClientInfo().initial_query_id);
    }

    explicit FunctionInitialQueryID(const String & initial_query_id_) : initial_query_id(initial_query_id_) {}

    inline String getName() const override { return name; }

    inline size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    inline bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, initial_query_id);
    }
};

void registerFunctionInitialQueryID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionInitialQueryID>();
    factory.registerAlias("initial_query_id", FunctionInitialQueryID::name, FunctionFactory::CaseInsensitive);
}
}
