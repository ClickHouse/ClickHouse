#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{

class FunctionTcpPort : public IFunction
{
public:
    static constexpr auto name = "tcpPort";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionTcpPort>(context, context->getTCPPort());
    }

    explicit FunctionTcpPort(ContextPtr context_, UInt16 port_) : context(context_), port(port_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt16>(); }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForConstantFolding() const override { return !context->isDistributed(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt16().createColumnConst(input_rows_count, port);
    }

private:
    ContextPtr context;
    const UInt64 port;
};

}

void registerFunctionTcpPort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTcpPort>();
}

}
