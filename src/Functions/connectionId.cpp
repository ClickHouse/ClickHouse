#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Get the connection Id. It's used for MySQL handler only.
class FunctionConnectionId : public IFunction, WithConstContext
{
public:
    static constexpr auto name = "connectionId";

    explicit FunctionConnectionId(ContextConstPtr context_) : WithConstContext(context_) {}

    static FunctionPtr create(ContextConstPtr context_) { return std::make_shared<FunctionConnectionId>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, getContext()->getClientInfo().connection_id);
    }
};

void registerFunctionConnectionId(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConnectionId>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("connection_id", "connectionID", FunctionFactory::CaseInsensitive);
}

}
