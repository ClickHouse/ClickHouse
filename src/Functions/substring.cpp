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
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// If 'is_utf8' - measure offset and length in code points instead of bytes.
/// UTF8 variant is not available for FixedString arguments.
template <bool is_utf8>
class FunctionSubstring : public IFunction
{
public:
    static constexpr auto name = is_utf8 ? "substringUTF8" : "substring";
    static FunctionPtr create(ContextPtr)
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

        if ((is_utf8 && !isString(arguments[0])) || !isStringOrFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isNativeNumber(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !isNativeNumber(arguments[2]))
            throw Exception("Illegal type " + arguments[2]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    ColumnPtr executeForSource(const ColumnPtr & column_start, const ColumnPtr & column_length,
                          const ColumnConst * column_start_const, const ColumnConst * column_length_const,
                          Int64 start_value, Int64 length_value, Source && source,
                          size_t input_rows_count) const
    {
        auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetUnbounded(
                        source, StringSink(*col_res, input_rows_count), static_cast<size_t>(start_value - 1));
                else if (start_value < 0)
                    sliceFromRightConstantOffsetUnbounded(
                        source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(start_value));
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
                    sliceFromLeftConstantOffsetBounded(
                        source, StringSink(*col_res, input_rows_count), static_cast<size_t>(start_value - 1), length_value);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetBounded(
                        source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(start_value), length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, input_rows_count), *column_start, *column_length);
        }

        return col_res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_start = arguments[1].column;
        ColumnPtr column_length;

        if (number_of_arguments == 3)
            column_length = arguments[2].column;

        const ColumnConst * column_start_const = checkAndGetColumn<ColumnConst>(column_start.get());
        const ColumnConst * column_length_const = nullptr;

        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 start_value = 0;
        Int64 length_value = 0;

        if (column_start_const)
            start_value = column_start_const->getInt(0);
        if (column_length_const)
            length_value = column_length_const->getInt(0);

        if constexpr (is_utf8)
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, UTF8StringSource(*col), input_rows_count);
            else if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, ConstSource<UTF8StringSource>(*col_const), input_rows_count);
            else
                throw Exception(
                    "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, StringSource(*col), input_rows_count);
            else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, FixedStringSource(*col_fixed), input_rows_count);
            else if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, ConstSource<StringSource>(*col_const), input_rows_count);
            else if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
                return executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                                length_value, ConstSource<FixedStringSource>(*col_const_fixed), input_rows_count);
            else
                throw Exception(
                    "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

}

void registerFunctionSubstring(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubstring<false>>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("substr", "substring", FunctionFactory::CaseInsensitive);
    factory.registerAlias("mid", "substring", FunctionFactory::CaseInsensitive); /// from MySQL dialect

    factory.registerFunction<FunctionSubstring<true>>(FunctionFactory::CaseSensitive);
}

}
