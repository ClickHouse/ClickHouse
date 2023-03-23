#pragma once

#include <Core/DecimalFunctions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

struct Processor
{
    /// For operations with Integer/Float
    template <typename FromVectorType>
    void vectorConstant(const FromVectorType & vec_from, const UInt8 value_precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_from.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            format(vec_from[i], buf_to, value_precision);
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    template <typename FirstArgVectorType>
    void vectorVector(const FirstArgVectorType & vec_from, const ColumnVector<UInt8>::Container & vec_precision,
                      ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_from.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            format(vec_from[i], buf_to, vec_precision[i]);
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    template <typename FirstArgType>
    void constantVector(const FirstArgType & value_from, const ColumnVector<UInt8>::Container & vec_precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_precision.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            format(value_from, buf_to, vec_precision[i]);
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    /// For operations with Decimal
    template <typename FirstArgVectorType>
    void vectorConstant(const FirstArgVectorType & vec_from, const UInt8 value_precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to, const UInt8 from_scale) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_from.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            writeText(vec_from[i], from_scale, buf_to, true, true, static_cast<UInt32>(value_precision));
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }
        buf_to.finalize();
    }

    template <typename FirstArgVectorType>
    void vectorVector(const FirstArgVectorType & vec_from, const ColumnVector<UInt8>::Container & vec_precision,
                      ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to, const UInt8 from_scale) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_from.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            writeText(vec_from[i], from_scale, buf_to, true, true, static_cast<UInt32>(vec_precision[i]));
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    template <typename FirstArgType>
    void constantVector(const FirstArgType & value_from, const ColumnVector<UInt8>::Container & vec_precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & offsets_to, const UInt8 from_scale) const
    {
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        size_t input_rows_count = vec_precision.size();
        offsets_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            writeText(value_from, from_scale, buf_to, true, true, static_cast<UInt32>(vec_precision[i]));
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
    }
private:

    static void format(double value, DB::WriteBuffer & out, int precision)
    {
        DB::DoubleConverter<false>::BufferType buffer;
        double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

        const auto result = DB::DoubleConverter<false>::instance().ToFixed(value, precision, &builder);

        if (!result)
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

        out.write(buffer, builder.position());
    }
};

class FunctionFormatDecimal : public IFunction
{
public:
    static constexpr auto name = "formatDecimal";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatDecimal>(); }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNumber(*arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal first argument for formatDecimal function: got {}, expected numeric type",
                            arguments[0]->getName());

        if (!isUInt8(*arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal second argument for formatDecimal function: got {}, expected UInt8",
                            arguments[1]->getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        WhichDataType which_from(arguments[0].type.get());

        if (which_from.isUInt8())
            return executeType<UInt8>(arguments);
        else if (which_from.isUInt16())
            return executeType<UInt16>(arguments);
        else if (which_from.isUInt32())
            return executeType<UInt32>(arguments);
        else if (which_from.isUInt64())
            return executeType<UInt64>(arguments);
        else if (which_from.isUInt128())
            return executeType<UInt128>(arguments);
        else if (which_from.isUInt256())
            return executeType<UInt256>(arguments);
        else if (which_from.isInt8())
            return executeType<Int8>(arguments);
        else if (which_from.isInt16())
            return executeType<Int16>(arguments);
        else if (which_from.isInt32())
            return executeType<Int32>(arguments);
        else if (which_from.isInt64())
            return executeType<Int64>(arguments);
        else if (which_from.isInt128())
            return executeType<Int128>(arguments);
        else if (which_from.isInt256())
            return executeType<Int256>(arguments);
        else if (which_from.isFloat32())
            return executeType<Float32>(arguments);
        else if (which_from.isFloat64())
            return executeType<Float64>(arguments);
        else if (which_from.isDecimal32())
            return executeType<Decimal32>(arguments);
        else if (which_from.isDecimal64())
            return executeType<Decimal64>(arguments);
        else if (which_from.isDecimal128())
            return executeType<Decimal128>(arguments);
        else if (which_from.isDecimal256())
            return executeType<Decimal256>(arguments);

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[0].column->getName(), getName());
    }

private:
    template <typename T, std::enable_if_t<is_integer<T> || is_floating_point<T>, bool> = true>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        auto result_column_string = ColumnString::create();
        auto col_to = assert_cast<ColumnString *>(result_column_string.get());
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        const auto * from_col = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get());
        const auto * precision_col = checkAndGetColumn<ColumnVector<UInt8>>(arguments[1].column.get());
        const auto * from_col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * precision_col_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

        Processor processor;

        if (from_col)
        {
            if (precision_col_const)
                processor.vectorConstant(from_col->getData(), precision_col_const->template getValue<UInt8>(), data_to, offsets_to);
            else
                processor.vectorVector(from_col->getData(), precision_col->getData(), data_to, offsets_to);
        }
        else if (from_col_const)
        {
            processor.constantVector(from_col_const->template getValue<T>(), precision_col->getData(), data_to, offsets_to);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function formatDecimal",
                            arguments[0].column->getName());
        }

        return result_column_string;
    }

    template <typename T, std::enable_if_t<is_decimal<T>, bool> = true>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        auto result_column_string = ColumnString::create();
        auto col_to = assert_cast<ColumnString *>(result_column_string.get());
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        const auto * from_col = checkAndGetColumn<ColumnDecimal<T>>(arguments[0].column.get());
        const auto * precision_col = checkAndGetColumn<ColumnVector<UInt8>>(arguments[1].column.get());
        const auto * from_col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * precision_col_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

        UInt8 from_scale = from_col->getScale();
        Processor processor;

        if (from_col)
        {
            if (precision_col_const)
                processor.vectorConstant(from_col->getData(), precision_col_const->template getValue<UInt8>(), data_to, offsets_to, from_scale);
            else
                processor.vectorVector(from_col->getData(), precision_col->getData(), data_to, offsets_to, from_scale);
        }
        else if (from_col_const)
        {
            processor.constantVector(from_col_const->template getValue<T>(), precision_col->getData(), data_to, offsets_to, from_scale);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function formatDecimal",
                            arguments[0].column->getName());
        }

        return result_column_string;
    }
};

}
