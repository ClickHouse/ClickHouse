#pragma once

#include <Functions/IFunction.h>


namespace DB
{

/** Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
  * Used only as prerequisites for higher-order functions.
  */
class FunctionReplicate : public IFunction
{
public:
    static constexpr auto name = "replicate";
    static FunctionPtr create(const Context & context);

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

}
