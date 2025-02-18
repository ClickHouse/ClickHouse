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
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

enum class SubstringDirection : uint8_t
{
    Left,
    Right
};

template <bool is_utf8, SubstringDirection direction>
class FunctionLeftRight : public IFunction
{
public:
    static constexpr auto name = direction == SubstringDirection::Left
        ? (is_utf8 ? "leftUTF8" : "left")
        : (is_utf8 ? "rightUTF8" : "right");

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionLeftRight>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((is_utf8 && !isString(arguments[0])) || !isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isNativeNumber(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}",
                    arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
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
                sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), length_value);
            else
                sliceFromRightDynamicLength(source, StringSink(*col_res, input_rows_count), *column_length);
        }

        return col_res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_length = arguments[1].column;

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
