#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{
namespace
{

class FunctionInterpolateInternal : public IFunction
{
public:
    static constexpr auto name = "__interpolate";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionInterpolateInternal>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        return arguments[0].column;
    }
};

}

REGISTER_FUNCTION(InterpolateInternal)
{
    factory.registerFunction("__interpolate", [](ContextPtr context){ return FunctionInterpolateInternal::create(context); }, {}, FunctionFactory::Case::Sensitive);
}

}
