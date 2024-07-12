#pragma once
#include <Columns/ColumnConst.h>
#include <Common/MemorySanitizer.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Base58.h>
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

        size_t max_result_size = static_cast<size_t>(ceil(2 * src_column.getChars().size() + 1));

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        const auto * src = src_column.getChars().data();
        auto * dst = dst_data.data();

        size_t prev_src_offset = 0;
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t current_src_offset = src_offsets[row];
            size_t src_length = current_src_offset - prev_src_offset - 1;
            size_t encoded_size = encodeBase58(&src[prev_src_offset], src_length, &dst[current_dst_offset]);
            prev_src_offset = current_src_offset;
            current_dst_offset += encoded_size;
            dst[current_dst_offset] = 0;
            ++current_dst_offset;

            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }
};

enum class Base58DecodeErrorHandling : uint8_t
{
    ThrowException,
    ReturnEmptyString
};

template <typename Name, Base58DecodeErrorHandling ErrorHandling>
struct Base58Decode
{
    static constexpr auto name = Name::name;

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

        size_t prev_src_offset = 0;
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t current_src_offset = src_offsets[row];
            size_t src_length = current_src_offset - prev_src_offset - 1;
            std::optional<size_t> decoded_size = decodeBase58(&src[prev_src_offset], src_length, &dst[current_dst_offset]);
            if (!decoded_size)
            {
                if constexpr (ErrorHandling == Base58DecodeErrorHandling::ThrowException)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Base58 value, cannot be decoded");
                else
                    decoded_size = 0;
            }

            prev_src_offset = current_src_offset;
            current_dst_offset += *decoded_size;
            dst[current_dst_offset] = 0;
            ++current_dst_offset;

            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }
};

template <typename Func>
class FunctionBase58Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBase58Conversion>(); }
    String getName() const override { return Func::name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of arguments for function {}: 1 expected.", getName());

        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}. Must be String.",
                arguments[0].type->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_string = arguments[0].column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());
        if (!input)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}, must be String",
                arguments[0].column->getName(), getName());

        auto dst_column = ColumnString::create();

        Func::process(*input, dst_column, input_rows_count);

        return dst_column;
    }
};
}
