#pragma once
#include <Columns/ColumnConst.h>
#include <Common/MemorySanitizer.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/base58.h>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

struct Base58Encode
{
    static constexpr auto name = "base58Encode";

    static void process(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and we take double scale to avoid any reallocation.

        size_t max_result_size = ceil(2 * src_column.getChars().size() + 1);

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        const auto * src = src_column.getChars().data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev;
            auto encoded_size = encodeBase58(src, dst_pos);

            src += srclen;
            dst_pos += encoded_size;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);
    }
};

struct Base58Decode
{
    static constexpr auto name = "base58Decode";

    static void process(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and decoded value will be no longer than source.

        size_t max_result_size = src_column.getChars().size() + 1;

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        const auto * src = src_column.getChars().data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev;

            auto decoded_size = decodeBase58(src, dst_pos);
            if (!decoded_size)
                throw Exception("Invalid Base58 value, cannot be decoded", ErrorCodes::BAD_ARGUMENTS);

            src += srclen;
            dst_pos += decoded_size;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);
    }
};

template <typename Func>
class FunctionBase58Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBase58Conversion>();
    }

    String getName() const override
    {
        return Func::name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Wrong number of arguments for function " + getName() + ":  1 expected.", ErrorCodes::BAD_ARGUMENTS);

        if (!isString(arguments[0].type))
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_string = arguments[0].column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());
        if (!input)
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName() + ", must be String",
                ErrorCodes::ILLEGAL_COLUMN);

        auto dst_column = ColumnString::create();

        Func::process(*input, dst_column, input_rows_count);

        return dst_column;
    }
};
}
