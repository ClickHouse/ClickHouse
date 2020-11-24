#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/UnicodeBar.h>
#include <Common/FieldVisitors.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** bar(x, min, max, width) - draws a strip from the number of characters proportional to (x - min) and equal to width for x == max.
  * Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
    static constexpr auto name = "bar";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBar>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception("Function " + getName()
                    + " requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. Passed "
                    + toString(arguments.size())
                    + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNativeNumber(arguments[0]) || !isNativeNumber(arguments[1]) || !isNativeNumber(arguments[2])
            || (arguments.size() == 4 && !isNativeNumber(arguments[3])))
            throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        Int64 min = extractConstant<Int64>(arguments, 1, "Second"); /// The level at which the line has zero length.
        Int64 max = extractConstant<Int64>(arguments, 2, "Third"); /// The level at which the line has the maximum length.

        /// The maximum width of the bar in characters, by default.
        Float64 max_width = arguments.size() == 4 ? extractConstant<Float64>(arguments, 3, "Fourth") : 80;

        if (max_width < 1)
            throw Exception("Max_width argument must be >= 1.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (max_width > 1000)
            throw Exception("Too large max_width.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        const auto & src = *arguments[0].column;

        auto res_column = ColumnString::create();

        if (executeNumber<UInt8>(src, *res_column, min, max, max_width)
            || executeNumber<UInt16>(src, *res_column, min, max, max_width)
            || executeNumber<UInt32>(src, *res_column, min, max, max_width)
            || executeNumber<UInt64>(src, *res_column, min, max, max_width)
            || executeNumber<Int8>(src, *res_column, min, max, max_width)
            || executeNumber<Int16>(src, *res_column, min, max, max_width)
            || executeNumber<Int32>(src, *res_column, min, max, max_width)
            || executeNumber<Int64>(src, *res_column, min, max, max_width)
            || executeNumber<Float32>(src, *res_column, min, max, max_width)
            || executeNumber<Float64>(src, *res_column, min, max, max_width))
        {
            return res_column;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    T extractConstant(const ColumnsWithTypeAndName & arguments, size_t argument_pos, const char * which_argument) const
    {
        const auto & column = *arguments[argument_pos].column;

        if (!isColumnConst(column))
            throw Exception(
                which_argument + String(" argument for function ") + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        return applyVisitor(FieldVisitorConvertToNumber<T>(), column[0]);
    }

    template <typename T>
    static void fill(const PaddedPODArray<T> & src,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_offsets,
        Int64 min,
        Int64 max,
        Float64 max_width)
    {
        size_t size = src.size();
        size_t current_offset = 0;

        dst_offsets.resize(size);
        dst_chars.reserve(size * (UnicodeBar::getWidthInBytes(max_width) + 1)); /// lines 0-terminated.

        for (size_t i = 0; i < size; ++i)
        {
            Float64 width = UnicodeBar::getWidth(src[i], min, max, max_width);
            size_t next_size = current_offset + UnicodeBar::getWidthInBytes(width) + 1;
            dst_chars.resize(next_size);
            UnicodeBar::render(width, reinterpret_cast<char *>(&dst_chars[current_offset]));
            current_offset = next_size;
            dst_offsets[i] = current_offset;
        }
    }

    template <typename T>
    static void fill(T src, String & dst_chars, Int64 min, Int64 max, Float64 max_width)
    {
        Float64 width = UnicodeBar::getWidth(src, min, max, max_width);
        dst_chars.resize(UnicodeBar::getWidthInBytes(width));
        UnicodeBar::render(width, dst_chars.data());
    }

    template <typename T>
    static bool executeNumber(const IColumn & src, ColumnString & dst, Int64 min, Int64 max, Float64 max_width)
    {
        if (const ColumnVector<T> * col = checkAndGetColumn<ColumnVector<T>>(&src))
        {
            fill(col->getData(), dst.getChars(), dst.getOffsets(), min, max, max_width);
            return true;
        }
        else
            return false;
    }
};

}

void registerFunctionBar(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBar>();
}

}
