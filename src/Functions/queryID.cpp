#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

namespace DB
{
class FunctionQueryID : public IFunction
{
    const String query_id;

public:
    static constexpr auto name = "queryID";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionQueryID>(context->getClientInfo().current_query_id);
    }

    explicit FunctionQueryID(const String & query_id_) : query_id(query_id_) {}

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
        return DataTypeString().createColumnConst(input_rows_count, query_id)->convertToFullColumnIfConst();
    }
};

REGISTER_FUNCTION(QueryID)
{
    factory.registerFunction<FunctionQueryID>();
    factory.registerAlias("query_id", FunctionQueryID::name, FunctionFactory::CaseInsensitive);
}
}
