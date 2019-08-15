#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// Implements function, giving value for column within range of given
// Example:
// | c1 |
// | 10 |
// | 20 |
// SELECT c1, neighbour(c1, 1) as c2:
// | c1 | c2 |
// | 10 | 20 |
// | 20 | 0  |
class FunctionNeighbour : public IFunction
{
public:
    static constexpr auto name = "neighbour";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionNeighbour>(context); }

    FunctionNeighbour(const Context & context_) : context(context_) {}

    /// Get the name of the function.
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                    + ", should be from 2 to 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        // second argument must be an integer
        if (!isInteger(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + " - should be an integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        else if (arguments[1]->isNullable())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + " - can not be Nullable",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


        // check that default value column has supertype with first argument
        if (number_of_arguments == 3)
        {
            DataTypes types = {arguments[0], arguments[2]};
            try
            {
                return getLeastSupertype(types);
            }
            catch (const Exception &)
            {
                throw Exception(
                    "Illegal types of arguments (" + types[0]->getName() + ", " + types[1]->getName()
                        + ")"
                          " of function "
                        + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return arguments[0];
    }

    static void insertDefaults(const MutableColumnPtr & target, size_t row_count, ColumnPtr & default_values_column, size_t offset)
    {
        if (row_count == 0)
        {
            return;
        }
        if (default_values_column)
        {
            if (isColumnConst(*default_values_column))
            {
                Field constant_value = (*default_values_column)[0];
                for (size_t row = 0; row < row_count; row++)
                {
                    target->insert(constant_value);
                }
            }
            else
            {
                target->insertRangeFrom(*default_values_column, offset, row_count);
            }
        }
        else
        {
            for (size_t row = 0; row < row_count; row++)
            {
                target->insertDefault();
            }
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto offset_structure = block.getByPosition(arguments[1]);

        ColumnPtr & offset_column = offset_structure.column;

        auto is_constant_offset = isColumnConst(*offset_structure.column);
        ColumnPtr default_values_column = nullptr;
        if (arguments.size() == 3)
        {
            default_values_column = block.getByPosition(arguments[2]).column;
        }

        ColumnWithTypeAndName & source_column_name_and_type = block.getByPosition(arguments[0]);
        DataTypes types = {source_column_name_and_type.type};
        if (default_values_column)
        {
            types.push_back(block.getByPosition(arguments[2]).type);
        }
        const DataTypePtr & result_type = getLeastSupertype(types);
        auto source_column = source_column_name_and_type.column;

        // adjust source and default values columns to resulting datatype
        if (!source_column_name_and_type.type->equals(*result_type))
        {
            source_column = castColumn(source_column_name_and_type, result_type, context);
        }

        if (default_values_column && !block.getByPosition(arguments[2]).type->equals(*result_type))
        {
            default_values_column = castColumn(block.getByPosition(arguments[2]), result_type, context);
        }

        // since we are working with both signed and unsigned - we'll try to use Int64 for handling all of them
        const DataTypePtr desired_type = std::make_shared<DataTypeInt64>();
        if (!block.getByPosition(arguments[1]).type->equals(*desired_type))
        {
            offset_column = castColumn(offset_structure, desired_type, context);
        }

        if (isColumnConst(*source_column))
        {
            auto column = result_type->createColumnConst(input_rows_count, (*source_column)[0]);
            block.getByPosition(result).column = std::move(column);
        }
        else
        {
            auto column = result_type->createColumn();
            column->reserve(input_rows_count);
            // with constant offset - insertRangeFrom
            if (is_constant_offset)
            {
                Int64 offset_value = offset_column->getInt(0);

                auto offset_value_casted = static_cast<size_t>(std::abs(offset_value));
                size_t default_value_count = std::min(offset_value_casted, input_rows_count);
                if (offset_value > 0)
                {
                    // insert shifted value
                    if (offset_value_casted <= input_rows_count)
                    {
                        column->insertRangeFrom(*source_column, offset_value_casted, input_rows_count - offset_value_casted);
                    }
                    insertDefaults(column, default_value_count, default_values_column, input_rows_count - default_value_count);
                }
                else if (offset_value < 0)
                {
                    // insert defaults up to offset_value
                    insertDefaults(column, default_value_count, default_values_column, 0);
                    column->insertRangeFrom(*source_column, 0, input_rows_count - default_value_count);
                }
                else
                {
                    // populate column with source values, when offset is equal to zero
                    column->insertRangeFrom(*source_column, 0, input_rows_count);
                }
            }
            else
            {
                // with dynamic offset - handle row by row
                for (size_t row = 0; row < input_rows_count; row++)
                {
                    Int64 offset_value = offset_column->getInt(row);
                    if (offset_value == 0)
                    {
                        column->insertFrom(*source_column, row);
                    }
                    else if (offset_value > 0)
                    {
                        size_t real_offset = row + offset_value;
                        if (real_offset > input_rows_count)
                        {
                            if (default_values_column)
                            {
                                column->insertFrom(*default_values_column, row);
                            }
                            else
                            {
                                column->insertDefault();
                            }
                        }
                        else
                        {
                            column->insertFrom(*source_column, real_offset);
                        }
                    }
                    else
                    {
                        // out of range
                        auto offset_value_casted = static_cast<size_t>(std::abs(offset_value));
                        if (offset_value_casted > row)
                        {
                            if (default_values_column)
                            {
                                column->insertFrom(*default_values_column, row);
                            }
                            else
                            {
                                column->insertDefault();
                            }
                        }
                        else
                        {
                            column->insertFrom(*source_column, row - offset_value_casted);
                        }
                    }
                }
            }
            block.getByPosition(result).column = std::move(column);
        }
    }

private:
    const Context & context;
};

void registerFunctionNeighbour(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNeighbour>();
}

}
