#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Get the connection ID. It's used for MySQL handler only.
class FunctionConnectionID : public IFunction, WithContext
{
public:
    static constexpr auto name = "connectionID";

    explicit FunctionConnectionID(ContextPtr context_) : WithContext(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionConnectionID>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, getContext()->getClientInfo().connection_id);
    }
};

void registerFunctionConnectionID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConnectionID>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("connection_id", "connectionID", FunctionFactory::CaseInsensitive);
}

}
