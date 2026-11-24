#pragma once

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

enum class SubstringDirection : uint8_t
{
    Left,
    Right
};

template <bool is_utf8, SubstringDirection direction>
class FunctionLeftRight final : public IFunction
{
public:
    static constexpr auto name = direction == SubstringDirection::Left
        ? (is_utf8 ? "leftUTF8" : "left")
        : (is_utf8 ? "rightUTF8" : "right");

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionLeftRight>();
    }

    [[nodiscard]] String getName() const override
    {
        return name;
    }

    [[nodiscard]] bool isVariadic() const override { return false; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    /// A row whose result is NULL, or an unused dictionary entry of a `LowCardinality` argument, may hold a UInt64 that
    /// does not fit Int64, and `executeImpl` would throw for it
    [[nodiscard]] bool canBeExecutedOnDefaultArguments(const ColumnsWithTypeAndName & arguments) const override
    {
        return !uint64ArgumentMayHoldUnusedValues(arguments);
    }

    [[nodiscard]] String getSignatureString() const override
    {
        if constexpr (is_utf8)
            return "(String, NativeNumber) -> String";
        else
            return "(StringOrFixedString, NativeNumber) -> String";
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    ColumnPtr executeForSource(const ColumnPtr & column_length,
                          const ColumnConst * column_length_const,
                          Int64 length_value, Source && source,
                          size_t input_rows_count) const
    {
        auto col_res = ColumnString::create();

        if constexpr (direction == SubstringDirection::Left)
        {
            if (column_length_const)
                sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), 0, length_value);
            else
                sliceFromLeftDynamicLength(source, StringSink(*col_res, input_rows_count), *column_length);
        }
        else
        {
            if (column_length_const)
            {
                if (length_value >= 0)
                {
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), length_value);
                }
                // According to the docs, if length_value < 0, we need to take a suffix of the string starting from the position abs(length_value)
                else
                {
                    /// Unsigned negation, so INT64_MIN gives 2^63 instead of overflowing; it skips the whole string.
                    const UInt64 abs_length_value = 0 - static_cast<UInt64>(length_value);
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), abs_length_value);
                }
            }
            else
            {
                /// Per-row handling so that negative lengths use the same code path as the
                /// constant-length case: `getSliceFromLeft(|length|)` (unbounded). The
                /// previous `sliceFromRightDynamicLength` reformulated `right(s, -k)` as
                /// `getSliceFromRight(elemSize - k, elemSize - k)`, which on a UTF-8 source
                /// trips an internal inconsistency for invalid UTF-8 input: `countCodePoints`
                /// (used for `elemSize`) skips lone continuation bytes while
                /// `skipCodePointsForward/Backward` treats them as 1-byte code points, so
                /// the result silently lost trailing bytes.
                StringSink sink(*col_res, input_rows_count);
                while (!source.isEnd())
                {
                    const size_t row = source.rowNum();
                    const Int64 length = column_length->getInt(row);
                    typename std::decay_t<decltype(source)>::Slice slice;
                    if (length > 0)
                        slice = source.getSliceFromRight(static_cast<UInt64>(length), static_cast<UInt64>(length));
                    else if (length < 0)
                    {
                        const UInt64 abs_length = 0 - static_cast<UInt64>(length);
                        slice = source.getSliceFromLeft(abs_length);
                    }
                    if (length != 0)
                        writeSlice(slice, sink);
                    sink.next();
                    source.next();
                }
            }
        }

        return col_res;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_length = arguments[1].column;

        /// The length is read with IColumn::getInt() below, so a UInt64 value that does not fit
        /// Int64 must be rejected here rather than read as a negative length.
        checkIntegerArgumentFitsInt64(*column_length, name, "length");

        const ColumnConst * column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 length_value = 0;

        if (column_length_const)
            length_value = column_length_const->getInt(0);

        if constexpr (is_utf8)
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
                return executeForSource(column_length, column_length_const,
                    length_value, UTF8StringSource(*col), input_rows_count);
            if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
                return executeForSource(
                    column_length, column_length_const, length_value, ConstSource<UTF8StringSource>(*col_const), input_rows_count);
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
        }
        else
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
                return executeForSource(column_length, column_length_const,
                    length_value, StringSource(*col), input_rows_count);
            if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_string.get()))
                return executeForSource(column_length, column_length_const, length_value, FixedStringSource(*col_fixed), input_rows_count);
            if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
                return executeForSource(
                    column_length, column_length_const, length_value, ConstSource<StringSource>(*col_const), input_rows_count);
            if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
                return executeForSource(
                    column_length, column_length_const, length_value, ConstSource<FixedStringSource>(*col_const_fixed), input_rows_count);
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
        }
    }
};

}
