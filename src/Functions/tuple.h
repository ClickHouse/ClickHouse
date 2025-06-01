#pragma once

#include <Functions/IFunction.h>

#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/isUnquotedIdentifier.h>

namespace DB
{

/** tuple(x, y, ...) is a function that allows you to group several columns.
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */
class FunctionTuple : public IFunction
{
    bool enable_named_columns;

public:
    static constexpr auto name = "tuple";

    static FunctionPtr create(ContextPtr context);

    explicit FunctionTuple(bool enable_named_columns_ = false) : enable_named_columns(enable_named_columns_) { }

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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            return std::make_shared<DataTypeTuple>(DataTypes{});

        DataTypes types;
        Names names;
        NameSet name_set;
        for (const auto & argument : arguments)
        {
            types.emplace_back(argument.type);
            names.emplace_back(argument.name);
            name_set.emplace(argument.name);
        }

        if (enable_named_columns && name_set.size() == names.size()
            && std::all_of(names.cbegin(), names.cend(), [](const auto & n) { return isUnquotedIdentifier(n); }))
            return std::make_shared<DataTypeTuple>(types, names);
        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty())
            return ColumnTuple::create(input_rows_count);

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
        return ColumnTuple::create(tuple_columns);
    }
};

}
