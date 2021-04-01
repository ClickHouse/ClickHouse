#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Common/DNSResolver.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
    static constexpr auto name = "hostName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionHostName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForConstantFolding() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /** convertToFullColumn needed because in distributed query processing,
      *    each server returns its own value.
      */
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(
            input_rows_count, DNSResolver::instance().getHostName())->convertToFullColumnIfConst();
    }
};

}

void registerFunctionHostName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHostName>();
    factory.registerAlias("hostname", "hostName");
}

}
