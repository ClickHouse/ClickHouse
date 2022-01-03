#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
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

class FunctionBitSlice : public IFunction
{
    const UInt8 word_size = 8;

public:
    static constexpr auto name = "bitSlice";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitSlice>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0]) && !isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (arguments[0]->onlyNull())
            return arguments[0];

        if (!isNativeNumber(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !isNativeNumber(arguments[2]))
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
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


        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            return executeForSource(
                column_start,
                column_length,
                column_start_const,
                column_length_const,
                start_value,
                length_value,
                StringSource(*col),
                input_rows_count);
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            return executeForSource(
                column_start,
                column_length,
                column_start_const,
                column_length_const,
                start_value,
                length_value,
                FixedStringSource(*col_fixed),
                input_rows_count);
        else if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
            return executeForSource(
                column_start,
                column_length,
                column_start_const,
                column_length_const,
                start_value,
                length_value,
                ConstSource<StringSource>(*col_const),
                input_rows_count);
        else if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            return executeForSource(
                column_start,
                column_length,
                column_start_const,
                column_length_const,
                start_value,
                length_value,
                ConstSource<FixedStringSource>(*col_const_fixed),
                input_rows_count);
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <class Source>
    ColumnPtr executeForSource(
        const ColumnPtr & column_start,
        const ColumnPtr & column_length,
        const ColumnConst * column_start_const,
        const ColumnConst * column_length_const,
        Int64 start_value,
        Int64 length_value,
        Source && source,
        size_t input_rows_count) const
    {
        auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    bitSliceFromLeftConstantOffsetUnbounded(
                        source, StringSink(*col_res, input_rows_count), static_cast<size_t>(start_value - 1));
                else if (start_value < 0)
                    bitSliceFromRightConstantOffsetUnbounded(
                        source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(start_value));
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                bitSliceDynamicOffsetUnbounded(source, StringSink(*col_res, input_rows_count), *column_start);
        }
        else
        {
            if (column_start_const && column_length_const)
            {
                if (start_value > 0)
                    bitSliceFromLeftConstantOffsetBounded(
                        source, StringSink(*col_res, input_rows_count), static_cast<size_t>(start_value - 1), length_value);
                else if (start_value < 0)
                    bitSliceFromRightConstantOffsetBounded(
                        source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(start_value), length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                bitSliceDynamicOffsetBounded(source, StringSink(*col_res, input_rows_count), *column_start, *column_length);
        }

        return col_res;
    }

    void writeSliceWithLeftShift(const StringSource::Slice & slice, StringSink & sink, size_t shift_bit, size_t abandon_last_bit = 0) const
    {
        if (!shift_bit && !abandon_last_bit)
        {
            writeSlice(slice, sink);
            return;
        }
        size_t size = slice.size;
        if (!size)
            return;
        bool abandon_last_byte = abandon_last_bit + shift_bit >= word_size;
        if (abandon_last_byte) // shift may eliminate last byte
            size--;
        sink.elements.resize(sink.current_offset + size);
        UInt8 * out = &sink.elements[sink.current_offset];
        const UInt8 * input = slice.data;

        for (size_t i = 0; i < size - 1; i++)
        {
            out[i] = (input[i] << shift_bit) | (input[i + 1] >> (word_size - shift_bit));
        }
        if (abandon_last_byte)
        {
            out[size - 1] = (input[size - 1] << shift_bit) | (input[size] >> (word_size - shift_bit));
            out[size - 1] = out[size - 1] & (0xFF << (abandon_last_bit + shift_bit - word_size));
        }
        else
        {
            out[size - 1] = (input[size - 1] << shift_bit) & (0xFF << (abandon_last_bit + shift_bit));
        }


        sink.current_offset += size;
    }


    template <class Source>
    void bitSliceFromLeftConstantOffsetUnbounded(Source && src, StringSink && sink, size_t offset) const
    {
        size_t offset_byte = offset / word_size;
        size_t offset_bit = offset % word_size;
        while (!src.isEnd())
        {
            auto sl = src.getSliceFromLeft(offset_byte);
            if (sl.size)
                writeSliceWithLeftShift(sl, sink, offset_bit);

            sink.next();
            src.next();
        }
    }

    template <class Source>
    void bitSliceFromRightConstantOffsetUnbounded(Source && src, StringSink && sink, size_t offset) const
    {
        size_t offset_byte = offset / word_size;
        size_t offset_bit = (word_size - (offset % word_size)) % word_size; // offset_bit always represent left offset bit
        if (offset_bit)
            offset_byte++;
        while (!src.isEnd())
        {
            auto slice = src.getSliceFromRight(offset_byte);
            size_t size = src.getElementSize();
            bool left_truncate = offset_byte > size;
            size_t shift_bit = left_truncate ? 0 : offset_bit;
            if (slice.size)
                writeSliceWithLeftShift(slice, sink, shift_bit);

            sink.next();
            src.next();
        }
    }

    template <class Source>
    void bitSliceDynamicOffsetUnbounded(Source && src, StringSink && sink, const IColumn & offset_column) const
    {
        const bool is_null = offset_column.onlyNull();
        const auto * nullable = typeid_cast<const ColumnNullable *>(&offset_column);
        const ColumnUInt8::Container * null_map = nullable ? &nullable->getNullMapData() : nullptr;
        const IColumn * nested_column = nullable ? &nullable->getNestedColumn() : &offset_column;

        while (!src.isEnd())
        {
            auto row_num = src.rowNum();
            bool has_offset = !is_null && (null_map && (*null_map)[row_num]);
            Int64 start = has_offset ? nested_column->getInt(row_num) : 1;
            if (start != 0)
            {
                typename std::decay_t<Source>::Slice slice;
                size_t shift_bit;

                if (start > 0)
                {
                    UInt64 offset = start - 1;
                    size_t offset_byte = offset / word_size;
                    size_t offset_bit = offset % word_size;
                    shift_bit = offset_bit;
                    slice = src.getSliceFromLeft(offset_byte);
                }
                else
                {
                    UInt64 offset = -static_cast<UInt64>(start);
                    size_t offset_byte = offset / word_size;
                    size_t offset_bit = (word_size - (offset % word_size)) % word_size; // offset_bit always represent left offset bit
                    if (offset_bit)
                        offset_byte++;
                    size_t size = src.getElementSize();
                    bool left_truncate = offset_byte > size;
                    shift_bit = left_truncate ? 0 : offset_bit;
                    slice = src.getSliceFromRight(offset_byte);
                }
                if (slice.size)
                    writeSliceWithLeftShift(slice, sink, shift_bit);
            }

            sink.next();
            src.next();
        }
    }

    template <class Source>
    void bitSliceFromLeftConstantOffsetBounded(Source && src, StringSink && sink, size_t offset, ssize_t length) const
    {
        size_t offset_byte = offset / word_size;
        size_t offset_bit = offset % word_size;
        size_t shift_bit = offset_bit;
        size_t length_byte = 0;
        size_t over_bit = 0;
        if (length > 0)
        {
            length_byte = (length + offset_bit) / word_size;
            over_bit = (length + offset_bit) % word_size;
            if (over_bit && (length_byte || over_bit > offset_bit)) // begin and end are not in same byte OR there are gaps
                length_byte++;
        }

        while (!src.isEnd())
        {
            ssize_t remain_byte = src.getElementSize() - offset_byte;
            if (length < 0)
            {
                length_byte = std::max(remain_byte + (length / word_size), static_cast<ssize_t>(0));
                over_bit = word_size + (length % word_size);
                if (length_byte == 1 && over_bit <= offset_bit) // begin and end are in same byte AND there are no gaps
                    length_byte = 0;
            }
            bool right_truncate = static_cast<ssize_t>(length_byte) > remain_byte;
            size_t abandon_last_bit = (over_bit && !right_truncate) ? word_size - over_bit : 0;
            auto slice = src.getSliceFromLeft(offset_byte, length_byte);
            if (slice.size)
                writeSliceWithLeftShift(slice, sink, shift_bit, abandon_last_bit);

            sink.next();
            src.next();
        }
    }


    template <class Source>
    void bitSliceFromRightConstantOffsetBounded(Source && src, StringSink && sink, size_t offset, ssize_t length) const
    {
        size_t offset_byte = offset / word_size;
        size_t offset_bit = (word_size - (offset % word_size)) % word_size; // offset_bit always represent left offset bit
        if (offset_bit)
            offset_byte++;
        size_t length_byte = 0;
        size_t over_bit = 0;
        if (length > 0)
        {
            length_byte = (length + offset_bit) / word_size;
            over_bit = (length + offset_bit) % word_size;
            if (over_bit && (length_byte || over_bit > offset_bit)) // begin and end are not in same byte OR there are gaps
                length_byte++;
        }

        while (!src.isEnd())
        {
            size_t size = src.getElementSize();
            if (length < 0)
            {
                length_byte = std::max(static_cast<ssize_t>(offset_byte) + (length / word_size), static_cast<ssize_t>(0));
                over_bit = word_size + (length % word_size);
                if (length_byte == 1 && over_bit <= offset_bit) // begin and end are in same byte AND there are no gaps
                    length_byte = 0;
            }
            bool left_truncate = offset_byte > size;
            bool right_truncate = length_byte > offset_byte;
            size_t shift_bit = left_truncate ? 0 : offset_bit;
            size_t abandon_last_bit = (over_bit && !right_truncate) ? word_size - over_bit : 0;
            auto slice = src.getSliceFromRight(offset_byte, length_byte);
            if (slice.size)
                writeSliceWithLeftShift(slice, sink, shift_bit, abandon_last_bit);

            sink.next();
            src.next();
        }
    }

    template <class Source>
    void bitSliceDynamicOffsetBounded(Source && src, StringSink && sink, const IColumn & offset_column, const IColumn & length_column) const
    {
        const bool is_offset_null = offset_column.onlyNull();
        const auto * offset_nullable = typeid_cast<const ColumnNullable *>(&offset_column);
        const ColumnUInt8::Container * offset_null_map = offset_nullable ? &offset_nullable->getNullMapData() : nullptr;
        const IColumn * offset_nested_column = offset_nullable ? &offset_nullable->getNestedColumn() : &offset_column;

        const bool is_length_null = length_column.onlyNull();
        const auto * length_nullable = typeid_cast<const ColumnNullable *>(&length_column);
        const ColumnUInt8::Container * length_null_map = length_nullable ? &length_nullable->getNullMapData() : nullptr;
        const IColumn * length_nested_column = length_nullable ? &length_nullable->getNestedColumn() : &length_column;


        while (!src.isEnd())
        {
            size_t row_num = src.rowNum();
            bool has_offset = !is_offset_null && !(offset_null_map && (*offset_null_map)[row_num]);
            bool has_length = !is_length_null && !(length_null_map && (*length_null_map)[row_num]);
            Int64 start = has_offset ? offset_nested_column->getInt(row_num) : 1;
            Int64 length = has_length ? length_nested_column->getInt(row_num) : static_cast<Int64>(src.getElementSize());

            if (start && length)
            {
                bool left_offset = start > 0;
                size_t offset = left_offset ? start - 1 : -start;
                size_t offset_byte;
                size_t offset_bit;
                size_t shift_bit;
                size_t length_byte;
                size_t over_bit;
                size_t size = src.getElementSize();

                if (left_offset)
                {
                    offset_byte = offset / word_size;
                    offset_bit = offset % word_size;
                    shift_bit = offset_bit;
                }
                else
                {
                    offset_byte = offset / word_size;
                    offset_bit = (word_size - (offset % word_size)) % word_size; // offset_bit always represent left offset bit
                    if (offset_bit)
                        offset_byte++;
                    bool left_truncate = offset_byte > size;
                    shift_bit = left_truncate ? 0 : offset_bit;
                }

                ssize_t remain_byte = left_offset ? size - offset_byte : offset_byte;
                if (length > 0)
                {
                    length_byte = (length + offset_bit) / word_size;
                    over_bit = (length + offset_bit) % word_size;
                    if (over_bit && (length_byte || (over_bit > offset_bit))) // begin and end are not in same byte OR there are gaps
                        length_byte++;
                }
                else
                {
                    length_byte = std::max(remain_byte + (static_cast<ssize_t>(length) / word_size), static_cast<ssize_t>(0));
                    over_bit = word_size + (length % word_size);
                    if (length_byte == 1 && over_bit <= offset_bit) // begin and end are in same byte AND there are no gaps
                        length_byte = 0;
                }

                bool right_truncate = static_cast<ssize_t>(length_byte) > remain_byte;
                size_t abandon_last_bit = (over_bit && !right_truncate) ? word_size - over_bit : 0;
                auto slice = left_offset ? src.getSliceFromLeft(offset_byte, length_byte) : src.getSliceFromRight(offset_byte, length_byte);
                if (slice.size)
                    writeSliceWithLeftShift(slice, sink, shift_bit, abandon_last_bit);
            }


            sink.next();
            src.next();
        }
    }
};


void registerFunctionBitSlice(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitSlice>();
}


}
