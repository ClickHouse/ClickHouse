#pragma once

#include <type_traits>
#include <Common/formatIPv6.h>
#include <Common/IPv6ToBinary.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_CONVERT_TYPE;
}

enum class IPStringToNumExceptionMode : uint8_t
{
    Throw,
    Default,
    Null
};

static inline bool tryParseIPv4(const char * pos, UInt32 & result_value)
{
    return parseIPv4whole(pos, reinterpret_cast<unsigned char *>(&result_value));
}

namespace detail
{
    template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv6, typename StringColumnType>
    ColumnPtr convertToIPv6(const StringColumnType & string_column, const PaddedPODArray<UInt8> * null_map = nullptr)
    {
        if constexpr (!std::is_same_v<ToColumn, ColumnFixedString> && !std::is_same_v<ToColumn, ColumnIPv6>)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal return column type {}. Expected IPv6 or FixedString",
                            TypeName<typename ToColumn::ValueType>);


        size_t column_size = string_column.size();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to = nullptr;

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(column_size, false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        /// This is a special treatment for source column of type FixedString(16)
        /// to preserve previous behavior when IPv6 was a domain type of FixedString(16)
        if constexpr (std::is_same_v<StringColumnType, ColumnFixedString>)
        {
            if (string_column.getN() == IPV6_BINARY_LENGTH)
            {
                if constexpr (std::is_same_v<ToColumn, ColumnFixedString>)
                {
                    auto col_res = ColumnFixedString::create(string_column);

                    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                    {
                        col_null_map_to = ColumnUInt8::create(column_size, false);
                        if (null_map)
                            memcpy(col_null_map_to->getData().data(), null_map->data(), column_size);
                        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
                    }

                    return col_res;
                }
                else
                {
                    auto col_res = ColumnIPv6::create();
                    auto & vec_res = col_res->getData();

                    vec_res.resize(column_size);
                    memcpy(vec_res.data(), string_column.getChars().data(), column_size * IPV6_BINARY_LENGTH);

                    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                    {
                        col_null_map_to = ColumnUInt8::create(column_size, false);
                        if (null_map)
                            memcpy(col_null_map_to->getData().data(), null_map->data(), column_size);
                        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
                    }

                    return col_res;
                }
            }
        }

        auto column_create = []() -> typename ToColumn::MutablePtr
        {
            if constexpr (std::is_same_v<ToColumn, ColumnFixedString>)
                return ColumnFixedString::create(IPV6_BINARY_LENGTH);
            else
                return ColumnIPv6::create();
        };

        auto get_vector = [](auto & col_res, size_t col_size) -> decltype(auto)
        {
            if constexpr (std::is_same_v<ToColumn, ColumnFixedString>)
            {
                auto & vec_res = col_res->getChars();
                vec_res.resize(col_size * IPV6_BINARY_LENGTH);
                return (vec_res);
            }
            else
            {
                auto & vec_res = col_res->getData();
                vec_res.resize(col_size);
                return (vec_res);
            }
        };

        auto col_res = column_create();
        auto & vec_res = get_vector(col_res, column_size);

        using Chars = typename StringColumnType::Chars;
        const Chars & vec_src = string_column.getChars();

        size_t src_offset = 0;
        char src_ipv4_buf[sizeof("::ffff:") + IPV4_MAX_TEXT_LENGTH + 1] = "::ffff:";

        /// ColumnFixedString contains not null terminated strings. But functions parseIPv6, parseIPv4 expect null terminated string.
        /// TODO fix this - now parseIPv6/parseIPv4 accept end iterator, so can be parsed in-place
        std::string fixed_string_buffer;

        if constexpr (std::is_same_v<StringColumnType, ColumnFixedString>)
        {
            fixed_string_buffer.resize(string_column.getN());
        }

        int offset_inc = 1;
        if constexpr (std::is_same_v<ToColumn, ColumnFixedString>)
            offset_inc = IPV6_BINARY_LENGTH;

        for (size_t out_offset = 0, i = 0; i < column_size; out_offset += offset_inc, ++i)
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

            if (null_map && (*null_map)[i])
            {
                std::fill_n(&vec_res[out_offset], offset_inc, 0);
                src_offset = src_next_offset;
                if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                    (*vec_null_map_to)[i] = true;
                continue;
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
                parse_result = parseIPv6whole(src_ipv4_buf, res_value);
            }
            else
            {
                parse_result = parseIPv6whole(src_value, res_value);
            }

            if (!parse_result)
            {
                if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
                    throw Exception(ErrorCodes::CANNOT_PARSE_IPV6, "Invalid IPv6 value");
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

template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv6>
ColumnPtr convertToIPv6(ColumnPtr column, const PaddedPODArray<UInt8> * null_map = nullptr)
{
    if (const auto * column_input_string = checkAndGetColumn<ColumnString>(column.get()))
    {
        return detail::convertToIPv6<exception_mode, ToColumn>(*column_input_string, null_map);
    }
    if (const auto * column_input_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get()))
    {
        return detail::convertToIPv6<exception_mode, ToColumn>(*column_input_fixed_string, null_map);
    }

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type {}. Expected String or FixedString", column->getName());
}

template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv4>
ColumnPtr convertToIPv4(ColumnPtr column, const PaddedPODArray<UInt8> * null_map = nullptr)
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

    auto col_res = ToColumn::create();

    auto & vec_res = col_res->getData();
    vec_res.resize(column_size);

    const ColumnString::Chars & vec_src = column_string->getChars();
    const ColumnString::Offsets & offsets_src = column_string->getOffsets();
    size_t prev_offset = 0;

    for (size_t i = 0; i < vec_res.size(); ++i)
    {
        if (null_map && (*null_map)[i])
        {
            vec_res[i] = 0;
            prev_offset = offsets_src[i];
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                (*vec_null_map_to)[i] = true;
            continue;
        }

        bool parse_result = tryParseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]), vec_res[i]);

        if (!parse_result)
        {
            if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_IPV4, "Invalid IPv4 value");
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

template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv4>
ColumnPtr convertIPv6ToIPv4(ColumnPtr column, const PaddedPODArray<UInt8> * null_map = nullptr)
{
    const ColumnIPv6 * column_ipv6 = checkAndGetColumn<ColumnIPv6>(column.get());

    if (!column_ipv6)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type {}. Expected IPv6.", column->getName());

    size_t column_size = column_ipv6->size();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
    {
        col_null_map_to = ColumnUInt8::create(column_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    const uint8_t ip4_cidr[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};

    auto col_res = ToColumn::create();
    auto & vec_res = col_res->getData();
    vec_res.resize(column_size);
    const auto & vec_src = column_ipv6->getData();

    for (size_t i = 0; i < vec_res.size(); ++i)
    {
        const uint8_t * src = reinterpret_cast<const uint8_t *>(&vec_src[i]);
        uint8_t * dst = reinterpret_cast<uint8_t *>(&vec_res[i]);

        if (null_map && (*null_map)[i])
        {
            std::memset(dst, '\0', IPV4_BINARY_LENGTH);
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                (*vec_null_map_to)[i] = true;
            continue;
        }

        if (!matchIPv6Subnet(src, ip4_cidr, 96))
        {
            if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
            {
                char addr[IPV6_MAX_TEXT_LENGTH + 1] {};
                char * paddr = addr;
                formatIPv6(src, paddr);

                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "IPv6 {} in column {} is not in IPv4 mapping block", addr, column->getName());
            }
            else if constexpr (exception_mode == IPStringToNumExceptionMode::Default)
            {
                std::memset(dst, '\0', IPV4_BINARY_LENGTH);
            }
            else if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
            {
                (*vec_null_map_to)[i] = true;
                std::memset(dst, '\0', IPV4_BINARY_LENGTH);
            }
            continue;
        }

        if constexpr (std::endian::native == std::endian::little)
        {
            dst[0] = src[15];
            dst[1] = src[14];
            dst[2] = src[13];
            dst[3] = src[12];
        }
        else
        {
            dst[0] = src[12];
            dst[1] = src[13];
            dst[2] = src[14];
            dst[3] = src[15];
        }
    }

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));

    return col_res;
}

}
