#pragma once

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Functions/IFunction.h>
#include <Functions/NumberTraits.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";
    static FunctionPtr create(const Context & context);

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) override
    {
        return true;
    }

    bool hasSpecialSupportForNulls() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


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

    bool hasSpecialSupportForNulls() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

}
