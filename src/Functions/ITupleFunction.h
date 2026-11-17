#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class ITupleFunction : public IFunction
{
protected:
    ContextPtr context;

public:
    explicit ITupleFunction(ContextPtr context_) : context(context_) {}

    Columns getTupleElements(const IColumn & column) const
    {
        if (const auto * const_column = typeid_cast<const ColumnConst *>(&column))
            return convertConstTupleToConstantElements(*const_column);

        if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
        {
            Columns columns(column_tuple->tupleSize());
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i] = column_tuple->getColumnPtr(i);
            return columns;
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} should be tuples, got {}",
                        getName(), column.getName());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
};
}
