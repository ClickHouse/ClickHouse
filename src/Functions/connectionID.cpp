#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Get the connection ID. It's used for MySQL handler only.
class FunctionConnectionID : public IFunction
{
public:
    static constexpr auto name = "connectionID";

    explicit FunctionConnectionID(const Context & context_) : context(context_) {}

    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionConnectionID>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, context.getClientInfo().connection_id);
    }

private:
    const Context & context;
};

void registerFunctionConnectionID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConnectionID>();
    factory.registerAlias("connection_id", "connectionID");
}

}
