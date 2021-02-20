#pragma once
#include <Functions/IFunctionImpl.h>

namespace DB
{

class Context;

/// Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
/// Function may accept more then two arguments. If so, the first array with non-empty offsets is chosen.
class FunctionReplicate : public IFunction
{
public:
    static constexpr auto name = "replicate";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReplicate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;
};

}
