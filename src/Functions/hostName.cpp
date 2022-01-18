#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Common/DNSResolver.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace
{

/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
    static constexpr auto name = "hostName";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionHostName>(context->isDistributed());
    }

    explicit FunctionHostName(bool is_distributed_) : is_distributed(is_distributed_)
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return true;
    }

    bool isSuitableForConstantFolding() const override { return !is_distributed; }

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
        return result_type->createColumnConst(input_rows_count, DNSResolver::instance().getHostName());
    }
private:
    bool is_distributed;
};

}

void registerFunctionHostName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHostName>();
    factory.registerAlias("hostname", "hostName");
}

}
