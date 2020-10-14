#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

class FunctionIdentity : public IFunction
{
public:
    static constexpr auto name = "identity";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIdentity>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.front();
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        columns[result].column = columns[arguments.front()].column;
    }
};

}

void registerFunctionIdentity(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIdentity>();
}

}
