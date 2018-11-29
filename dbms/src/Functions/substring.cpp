#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <IO/WriteHelpers.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionSubstring : public IFunction
{
public:
    static constexpr auto name = "substring";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstring>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(number_of_arguments) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isStringOrFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isNumber(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !isNumber(arguments[2]))
            throw Exception("Illegal type " + arguments[2]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    void executeForSource(const ColumnPtr & column_start, const ColumnPtr & column_length,
                              const ColumnConst * column_start_const, const ColumnConst * column_length_const,
                              Int64 start_value, Int64 length_value, Block & block, size_t result, Source && source,
                              size_t input_rows_count)
    {
       auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), start_value - 1);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), -start_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetUnbounded(source, StringSink(*col_res, input_rows_count), *column_start);
        }
        else
        {
            if (column_start_const && column_length_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), start_value - 1, length_value);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), -start_value, length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, input_rows_count), *column_start, *column_length);
        }

        block.getByPosition(result).column = std::move(col_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        ColumnPtr column_length;

        if (number_of_arguments == 3)
            column_length = block.getByPosition(arguments[2]).column;

        const ColumnConst * column_start_const = checkAndGetColumn<ColumnConst>(column_start.get());
        const ColumnConst * column_length_const = nullptr;

        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 start_value = 0;
        Int64 length_value = 0;

        if (column_start_const)
        {
            start_value = column_start_const->getInt(0);
        }
        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Third argument provided for function substring could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, StringSource(*col), input_rows_count);
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, FixedStringSource(*col), input_rows_count);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, ConstSource<StringSource>(*col), input_rows_count);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, ConstSource<FixedStringSource>(*col), input_rows_count);
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionSubstring(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubstring>();
}

}
