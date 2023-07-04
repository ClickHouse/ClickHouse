#pragma once
#include "config.h"

#if USE_BASE64
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/IFunction.h>
#    include <Interpreters/Context_fwd.h>
#    include <turbob64.h>
#    include <Common/MemorySanitizer.h>

#    include <span>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}

namespace Detail
{
    inline size_t base64Decode(const std::span<const UInt8> src, UInt8 * dst)
    {
#    if defined(__aarch64__)
        return tb64sdec(reinterpret_cast<const uint8_t *>(src.data()), src.size(), reinterpret_cast<uint8_t *>(dst));
#    else
        return _tb64d(reinterpret_cast<const uint8_t *>(src.data()), src.size(), reinterpret_cast<uint8_t *>(dst));
#    endif
    }
}

struct Base64Encode
{
    static constexpr auto name = "base64Encode";

    static size_t getBufferSize(const size_t string_length, const size_t string_count)
    {
        return ((string_length - string_count) / 3 + string_count) * 4 + string_count;
    }

    static size_t performCoding(const std::span<const UInt8> src, UInt8 * dst)
    {
        /*
        * Some bug in sse arm64 implementation?
        * `base64Encode(repeat('a', 46))` returns wrong padding character
        */
#    if defined(__aarch64__)
        return tb64senc(reinterpret_cast<const uint8_t *>(src.data()), src.size(), reinterpret_cast<uint8_t *>(dst));
#    else
        return _tb64e(reinterpret_cast<const uint8_t *>(src.data()), src.size(), reinterpret_cast<uint8_t *>(dst));
#    endif
    }
};

struct Base64Decode
{
    static constexpr auto name = "base64Decode";

    static size_t getBufferSize(const size_t string_length, const size_t string_count)
    {
        return ((string_length - string_count) / 4 + string_count) * 3 + string_count;
    }

    static size_t performCoding(const std::span<const UInt8> src, UInt8 * dst)
    {
        const auto outlen = Detail::base64Decode(src, dst);
        if (src.size() > 0 && !outlen)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Failed to {} input '{}'",
                name,
                String(reinterpret_cast<const char *>(src.data()), src.size()));

        return outlen;
    }
};

struct TryBase64Decode
{
    static constexpr auto name = "tryBase64Decode";

    static size_t getBufferSize(const size_t string_length, const size_t string_count)
    {
        return Base64Decode::getBufferSize(string_length, string_count);
    }

    static size_t performCoding(const std::span<const UInt8> src, UInt8 * dst)
    {
        if (src.empty())
            return 0;

        const auto outlen = Detail::base64Decode(src, dst);
        // during decoding character array can be partially polluted
        // if fail, revert back and clean
        if (!outlen)
            *dst = 0;

        return outlen;
    }
};

template <typename Func>
class FunctionBase64Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBase64Conversion>(); }
    String getName() const override { return Func::name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of arguments for function {}: 1 expected.", getName());

        if (!WhichDataType(arguments[0].type).isStringOrFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Must be FixedString or String.",
                arguments[0].type->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const override
    {
        const auto & input_column = arguments[0].column;
        if (const auto * src_column_as_fixed_string = checkAndGetColumn<ColumnFixedString>(*input_column))
            return execute(*src_column_as_fixed_string, input_rows_count);
        else if (const auto * src_column_as_string = checkAndGetColumn<ColumnString>(*input_column))
            return execute(*src_column_as_string, input_rows_count);

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of first argument of function {}, must be of type FixedString or String.",
            input_column->getName(),
            getName());
    }

private:
    static ColumnPtr execute(const ColumnString & src_column, const size_t src_row_count)
    {
        auto dst_column = ColumnString::create();
        auto & dst_chars = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        const auto reserve = Func::getBufferSize(src_column.byteSize(), src_column.size());
        dst_chars.resize(reserve);
        dst_offsets.resize(src_row_count);

        const auto & src_chars = src_column.getChars();
        const auto & src_offsets = src_column.getOffsets();

        auto * dst = dst_chars.data();
        auto * dst_pos = dst;
        const auto * src = src_chars.data();

        size_t src_offset_prev = 0;
        for (size_t row = 0; row < src_row_count; ++row)
        {
            const size_t src_length = src_offsets[row] - src_offset_prev - 1;
            const auto outlen = Func::performCoding({src, src_length}, dst_pos);

            /// Base64 library is using AVX-512 with some shuffle operations.
            /// Memory sanitizer don't understand if there was uninitialized memory in SIMD register but it was not used in the result of shuffle.
            __msan_unpoison(dst_pos, outlen);

            src += src_length + 1;
            dst_pos += outlen;
            *dst_pos = '\0';
            dst_pos += 1;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_chars.resize(dst_pos - dst);
        return dst_column;
    }

    static ColumnPtr execute(const ColumnFixedString & src_column, const size_t src_row_count)
    {
        auto dst_column = ColumnString::create();
        auto & dst_chars = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        const auto reserve = Func::getBufferSize(src_column.byteSize(), src_column.size());
        dst_chars.resize(reserve);
        dst_offsets.resize(src_row_count);

        const auto & src_chars = src_column.getChars();
        const auto & src_n = src_column.getN();

        auto * dst = dst_chars.data();
        auto * dst_pos = dst;
        const auto * src = src_chars.data();

        for (size_t row = 0; row < src_row_count; ++row)
        {
            const auto outlen = Func::performCoding({src, src_n}, dst_pos);

            /// Base64 library is using AVX-512 with some shuffle operations.
            /// Memory sanitizer don't understand if there was uninitialized memory in SIMD register but it was not used in the result of shuffle.
            __msan_unpoison(dst_pos, outlen);

            src += src_n;
            dst_pos += outlen;
            *dst_pos = '\0';
            dst_pos += 1;

            dst_offsets[row] = dst_pos - dst;
        }

        dst_chars.resize(dst_pos - dst);
        return dst_column;
    }
};
}

#endif
