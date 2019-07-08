#include <Columns/ColumnConst.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// Implements function, giving value for column in next row
// Example:
// | c1 |
// | 10 |
// | 20 |
// SELECT c1, nextInBlock(c1, 1) as c2:
// | c1 | c2 |
// | 10 | 20 |
// | 20 | 0  |
class FunctionNextInBlock : public IFunction
{
public:
    static constexpr auto name = "nextInBlock";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNextInBlock>(); }

    /// Get the name of the function.
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 1 || number_of_arguments > 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                    + ", should be from 1 to 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        // second argument must be a positive, constant column
        if (number_of_arguments == 2 && !isUnsignedInteger(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + " - should be positive integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // check that default value has supertype with first argument
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t offset_value = 1;

        if (arguments.size() > 1)
        {
            auto offset_column = block.getByPosition(arguments[1]);
            if (!isColumnConst(*offset_column.column))
                throw Exception("Second argument of function " + getName() + " should be constant", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            Field offset_field = (*block.getByPosition(arguments[1]).column)[0];
            auto raw_value = safeGet<UInt64>(offset_field);

            if (raw_value == 0)
                throw Exception(
                    "Second argument of function " + getName() + " should be positive integer, " + toString(raw_value) + " given",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            offset_value = raw_value;
        }

        auto has_column_for_missing = arguments.size() == 3;

        DataTypes types = {block.getByPosition(arguments[0]).type};
        if (has_column_for_missing)
        {
            types.push_back(block.getByPosition(arguments[2]).type);
        }
        const DataTypePtr & result_type = getLeastSupertype(types);

        auto column = result_type->createColumn();
        column->reserve(input_rows_count);

        auto source_column = block.getByPosition(arguments[0]).column;

        for (size_t i = offset_value; i < input_rows_count; i++)
        {
            column->insertFrom(*source_column, i);
        }

        if (has_column_for_missing)
        {
            auto default_values_column = block.getByPosition(arguments[2]).column;
            size_t starting_pos = offset_value > input_rows_count ? 0 : input_rows_count - offset_value;
            if (isColumnConst(*default_values_column))
            {
                Field constant_value = (*default_values_column)[0];
                for (size_t i = starting_pos; i < input_rows_count; i++)
                {
                    column->insert(constant_value);
                }
            }
            else
            {
                for (size_t i = starting_pos; i < input_rows_count; i++)
                {
                    column->insertFrom(*default_values_column, i);
                }
            }
        }
        else
        {
            for (size_t i = 0; i < std::min(offset_value, input_rows_count); i++)
            {
                column->insertDefault();
            }
        }

        block.getByPosition(result).column = std::move(column);
    }
};

void registerFunctionNextInBlock(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNextInBlock>();
}

}