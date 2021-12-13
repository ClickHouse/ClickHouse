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
        return std::make_shared<FunctionTcpPort>(context->getTCPPort());
    }

    explicit FunctionTcpPort(UInt16 port_) : port(port_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt16>(); }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt16().createColumnConst(input_rows_count, port);
    }

private:
    const UInt64 port;
};

}

void registerFunctionTcpPort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTcpPort>();
}

}
