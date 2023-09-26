#pragma once

#include <Columns/ColumnNothing.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** tuple(x, y, ...) is a function that allows you to group several columns
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */
class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";

    /// maybe_unused: false-positive
    [[ maybe_unused ]] static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTuple>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    /// tuple(..., Nothing, ...) -> Tuple(..., Nothing, ...)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeNothing>()});
        return std::make_shared<DataTypeTuple>(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t tuple_size = arguments.size();
        Columns tuple_columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            /** If tuple is mixed of constant and not constant columns,
                *  convert all to non-constant columns,
                *  because many places in code expect all non-constant columns in non-constant tuple.
                */
            tuple_columns[i] = arguments[i].column->convertToFullColumnIfConst();
        }

        if (tuple_columns.empty())
            tuple_columns.push_back(ColumnNothing::create(input_rows_count));

        return ColumnTuple::create(tuple_columns);
    }
};

}
