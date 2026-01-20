#pragma once

#include <Functions/IFunction.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** tuple(x, y, ...) is a function that allows you to group several columns.
  * tuple(name1, name2, ...)(x, y, ...) creates a named tuple with the given names.
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */
class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";

    static FunctionPtr create(ContextPtr /* context */) { return std::make_shared<FunctionTuple>(); }

    explicit FunctionTuple(const Array & parameters_ = {}) : parameters(parameters_) {}

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
        if (parameters.empty())
            return std::make_shared<DataTypeTuple>(arguments);

        /// Named tuple: tuple(name1, name2, ...)(value1, value2, ...)
        /// Validate that we have the correct number of names
        if (parameters.size() != arguments.size())
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of names ({}) does not match number of values ({}) in tuple function",
                parameters.size(),
                arguments.size());
        }

        /// Extract names from parameters and validate them
        Names param_names;
        param_names.reserve(parameters.size());

        for (size_t i = 0; i < parameters.size(); ++i)
        {
            const auto & param = parameters[i];

            /// Check that parameter is a String
            if (param.getType() != Field::Types::String)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Tuple element name must be string literals, got type {} for argument {}",
                    param.getTypeName(),
                    i);
            }

            String param_name = param.safeGet<String>();

            /// Validate that the name is a valid identifier
            if (param_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element name cannot be empty at position {}", i);

            /// Check for duplicate param_names
            for (size_t j = 0; j < i; ++j)
            {
                if (param_names[j] == param_name)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Duplicate tuple element name '{}' at positions {} and {}", param_name, j, i);
                }
            }

            param_names.push_back(param_name);
        }

        return std::make_shared<DataTypeTuple>(arguments, param_names);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty())
            return DataTypeTuple({}).createColumnConstWithDefaultValue(input_rows_count);

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

private:
    Array parameters;
};

}
