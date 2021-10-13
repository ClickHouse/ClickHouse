#pragma once
#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_BASE64
#    include <Columns/ColumnConst.h>
#    include <Common/MemorySanitizer.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/GatherUtils/Algorithms.h>
#    include <IO/WriteHelpers.h>
#    include <turbob64.h>


namespace DB
{
using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}

struct Base64Encode
{
    static constexpr auto name = "base64Encode";
    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return ((string_length - string_count) / 3 + string_count) * 4 + string_count;
    }
};

struct Base64Decode
{
    static constexpr auto name = "base64Decode";

    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return ((string_length - string_count) / 4 + string_count) * 3 + string_count;
    }
};

struct TryBase64Decode
{
    static constexpr auto name = "tryBase64Decode";

    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return Base64Decode::getBufferSize(string_length, string_count);
    }
};

template <typename Func>
class FunctionBase64Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBase64Conversion>();
    }

    String getName() const override
    {
        return Func::name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!WhichDataType(arguments[0].type).isString())
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_string = arguments[0].column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

        if (!input)
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto dst_column = ColumnString::create();
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        size_t reserve = Func::getBufferSize(input->getChars().size(), input->size());
        dst_data.resize(reserve);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input->getOffsets();

        const auto * source = input->getChars().data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;
            size_t outlen = 0;

            if constexpr (std::is_same_v<Func, Base64Encode>)
            {
                outlen = _tb64e(reinterpret_cast<const uint8_t *>(source), srclen, reinterpret_cast<uint8_t *>(dst_pos));
            }
            else if constexpr (std::is_same_v<Func, Base64Decode>)
            {
                if (srclen > 0)
                {
                    outlen = _tb64d(reinterpret_cast<const uint8_t *>(source), srclen, reinterpret_cast<uint8_t *>(dst_pos));
                    if (!outlen)
                        throw Exception("Failed to " + getName() + " input '" + String(reinterpret_cast<const char *>(source), srclen) + "'", ErrorCodes::INCORRECT_DATA);
                }
            }
            else
            {
                if (srclen > 0)
                {
                    // during decoding character array can be partially polluted
                    // if fail, revert back and clean
                    auto * savepoint = dst_pos;
                    outlen = _tb64d(reinterpret_cast<const uint8_t *>(source), srclen, reinterpret_cast<uint8_t *>(dst_pos));
                    if (!outlen)
                    {
                        outlen = 0;
                        dst_pos = savepoint; //-V1048
                        // clean the symbol
                        dst_pos[0] = 0;
                    }
                }
            }

            /// Base64 library is using AVX-512 with some shuffle operations.
            /// Memory sanitizer don't understand if there was uninitialized memory in SIMD register but it was not used in the result of shuffle.
            __msan_unpoison(dst_pos, outlen);

            source += srclen + 1;
            dst_pos += outlen;
            *dst_pos = '\0';
            dst_pos += 1;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);

        return dst_column;
    }
};
}

#endif
