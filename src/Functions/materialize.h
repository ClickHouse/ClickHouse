#pragma once
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

/** materialize(x) - materialize the constant
  */
class FunctionMaterialize : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMaterialize>();
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        return arguments[0].column->convertToFullColumnIfConst();
    }
};

}
