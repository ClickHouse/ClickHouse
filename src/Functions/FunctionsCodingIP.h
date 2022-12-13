#pragma once

#include <Common/formatIPv6.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int ILLEGAL_COLUMN;
}

enum class IPStringToNumExceptionMode : uint8_t
{
    Throw,
    Default,
    Null
};

static inline bool tryParseIPv4(const char * pos, UInt32 & result_value)
{
    return parseIPv4(pos, reinterpret_cast<unsigned char *>(&result_value));
}

namespace detail
{
    template <IPStringToNumExceptionMode exception_mode, typename StringColumnType>
    ColumnPtr convertToIPv6(const StringColumnType & string_column)
    {
        size_t column_size = string_column.size();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to = nullptr;

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(column_size, false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        auto col_res = ColumnFixedString::create(IPV6_BINARY_LENGTH);

        auto & vec_res = col_res->getChars();
        vec_res.resize(column_size * IPV6_BINARY_LENGTH);

        using Chars = typename StringColumnType::Chars;
        const Chars & vec_src = string_column.getChars();

        size_t src_offset = 0;
        char src_ipv4_buf[sizeof("::ffff:") + IPV4_MAX_TEXT_LENGTH + 1] = "::ffff:";

        /// ColumnFixedString contains not null terminated strings. But functions parseIPv6, parseIPv4 expect null terminated string.
        std::string fixed_string_buffer;

        if constexpr (std::is_same_v<StringColumnType, ColumnFixedString>)
        {
            fixed_string_buffer.resize(string_column.getN());
        }

        for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += IPV6_BINARY_LENGTH, ++i)
        {
            size_t src_next_offset = src_offset;

            const char * src_value = nullptr;
            unsigned char * res_value = reinterpret_cast<unsigned char *>(&vec_res[out_offset]);

            if constexpr (std::is_same_v<StringColumnType, ColumnString>)
            {
                src_value = reinterpret_cast<const char *>(&vec_src[src_offset]);
                src_next_offset = string_column.getOffsets()[i];
            }
            else if constexpr (std::is_same_v<StringColumnType, ColumnFixedString>)
            {
                size_t fixed_string_size = string_column.getN();

                std::memcpy(fixed_string_buffer.data(), reinterpret_cast<const char *>(&vec_src[src_offset]), fixed_string_size);
                src_value = fixed_string_buffer.data();

                src_next_offset += fixed_string_size;
            }

            bool parse_result = false;
            UInt32 dummy_result = 0;

            /// For both cases below: In case of failure, the function parseIPv6 fills vec_res with zero bytes.

            /// If the source IP address is parsable as an IPv4 address, then transform it into a valid IPv6 address.
            /// Keeping it simple by just prefixing `::ffff:` to the IPv4 address to represent it as a valid IPv6 address.
            if (tryParseIPv4(src_value, dummy_result))
            {
                std::memcpy(
                    src_ipv4_buf + std::strlen("::ffff:"),
                    src_value,
                    std::min<UInt64>(src_next_offset - src_offset, IPV4_MAX_TEXT_LENGTH + 1));
                parse_result = parseIPv6(src_ipv4_buf, res_value);
            }
            else
            {
                parse_result = parseIPv6(src_value, res_value);
            }

            if (!parse_result)
            {
                if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
                    throw Exception("Invalid IPv6 value", ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
                else if constexpr (exception_mode == IPStringToNumExceptionMode::Default)
                    vec_res[i] = 0;
                else if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                    (*vec_null_map_to)[i] = true;
            }

            src_offset = src_next_offset;
        }

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));

        return col_res;
    }
}

template <IPStringToNumExceptionMode exception_mode>
ColumnPtr convertToIPv6(ColumnPtr column)
{
    size_t column_size = column->size();

    auto col_res = ColumnFixedString::create(IPV6_BINARY_LENGTH);

    auto & vec_res = col_res->getChars();
    vec_res.resize(column_size * IPV6_BINARY_LENGTH);

    if (const auto * column_input_string = checkAndGetColumn<ColumnString>(column.get()))
    {
        return detail::convertToIPv6<exception_mode>(*column_input_string);
    }
    else if (const auto * column_input_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get()))
    {
        return detail::convertToIPv6<exception_mode>(*column_input_fixed_string);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type {}. Expected String or FixedString", column->getName());
    }
}

template <IPStringToNumExceptionMode exception_mode>
ColumnPtr convertToIPv4(ColumnPtr column)
{
    const ColumnString * column_string = checkAndGetColumn<ColumnString>(column.get());

    if (!column_string)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type {}. Expected String.", column->getName());
    }

    size_t column_size = column_string->size();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
    {
        col_null_map_to = ColumnUInt8::create(column_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto col_res = ColumnUInt32::create();

    ColumnUInt32::Container & vec_res = col_res->getData();
    vec_res.resize(column_size);

    const ColumnString::Chars & vec_src = column_string->getChars();
    const ColumnString::Offsets & offsets_src = column_string->getOffsets();
    size_t prev_offset = 0;

    for (size_t i = 0; i < vec_res.size(); ++i)
    {
        bool parse_result = tryParseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]), vec_res[i]);

        if (!parse_result)
        {
            if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
            {
                throw Exception("Invalid IPv4 value", ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
            }
            else if constexpr (exception_mode == IPStringToNumExceptionMode::Default)
            {
                vec_res[i] = 0;
            }
            else if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
            {
                (*vec_null_map_to)[i] = true;
                vec_res[i] = 0;
            }
        }

        prev_offset = offsets_src[i];
    }

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));

    return col_res;
}

}
