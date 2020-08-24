#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

// Implements function, giving value for column within range of given
// Example:
// | c1 |
// | 10 |
// | 20 |
// SELECT c1, neighbor(c1, 1) as c2:
// | c1 | c2 |
// | 10 | 20 |
// | 20 | 0  |
class FunctionNeighbor : public IFunction
{
public:
    static constexpr auto name = "neighbor";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNeighbor>(); }

    /// Get the name of the function.
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isStateful() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return false; }

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
            return getLeastSupertype({arguments[0], arguments[2]});

        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const DataTypePtr & result_type = block.getByPosition(result).type;

        const ColumnWithTypeAndName & source_elem = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & offset_elem = block.getByPosition(arguments[1]);
        bool has_defaults = arguments.size() == 3;

        ColumnPtr source_column_casted = castColumn(source_elem, result_type);
        ColumnPtr offset_column = offset_elem.column;

        ColumnPtr default_column_casted;
        if (has_defaults)
        {
            const ColumnWithTypeAndName & default_elem = block.getByPosition(arguments[2]);
            default_column_casted = castColumn(default_elem, result_type);
        }

        bool source_is_constant = isColumnConst(*source_column_casted);
        bool offset_is_constant = isColumnConst(*offset_column);

        bool default_is_constant = false;
        if (has_defaults)
             default_is_constant = isColumnConst(*default_column_casted);

        if (source_is_constant)
            source_column_casted = assert_cast<const ColumnConst &>(*source_column_casted).getDataColumnPtr();
        if (offset_is_constant)
            offset_column = assert_cast<const ColumnConst &>(*offset_column).getDataColumnPtr();
        if (default_is_constant)
            default_column_casted = assert_cast<const ColumnConst &>(*default_column_casted).getDataColumnPtr();

        if (offset_is_constant)
        {
            /// Optimization for the case when we can copy many values at once.

            Int64 offset = offset_column->getInt(0);

            /// Protection from possible overflow.
            if (unlikely(offset > (1 << 30) || offset < -(1 << 30)))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large offset: {} in function {}", offset, getName());

            auto result_column = result_type->createColumn();

            auto insert_range_from = [&](bool is_const, const ColumnPtr & src, Int64 begin, Int64 size)
            {
                /// Saturation of bounds.
                if (begin < 0)
                {
                    size += begin;
                    begin = 0;
                }
                if (size <= 0)
                    return;
                if (size > Int64(input_rows_count))
                    size = input_rows_count;

                if (!src)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertDefault();
                }
                else if (is_const)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertFrom(*src, 0);
                }
                else
                {
                    result_column->insertRangeFrom(*src, begin, size);
                }
            };

            if (offset == 0)
            {
                /// Degenerate case, just copy source column as is.
                block.getByPosition(result).column = source_is_constant
                    ? ColumnConst::create(source_column_casted, input_rows_count)
                    : source_column_casted;
            }
            else if (offset > 0)
            {
                insert_range_from(source_is_constant, source_column_casted, offset, Int64(input_rows_count) - offset);
                insert_range_from(default_is_constant, default_column_casted, Int64(input_rows_count) - offset, offset);
                block.getByPosition(result).column = std::move(result_column);
            }
            else
            {
                insert_range_from(default_is_constant, default_column_casted, 0, -offset);
                insert_range_from(source_is_constant, source_column_casted, 0, Int64(input_rows_count) + offset);
                block.getByPosition(result).column = std::move(result_column);
            }
        }
        else
        {
            auto result_column = result_type->createColumn();

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                Int64 offset = offset_column->getInt(offset_is_constant ? 0 : row);

                /// Protection from possible overflow.
                if (unlikely(offset > (1 << 30) || offset < -(1 << 30)))
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large offset: {} in function {}", offset, getName());

                Int64 src_idx = row + offset;

                if (src_idx >= 0 && src_idx < Int64(input_rows_count))
                    result_column->insertFrom(*source_column_casted, source_is_constant ? 0 : src_idx);
                else if (has_defaults)
                    result_column->insertFrom(*default_column_casted, default_is_constant ? 0 : row);
                else
                    result_column->insertDefault();
            }

            block.getByPosition(result).column = std::move(result_column);
        }
    }
};

void registerFunctionNeighbor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNeighbor>();
}

}
