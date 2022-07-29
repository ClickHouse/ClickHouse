#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include "Poco/Unicode.h"
#include <punycode.h>

#include <string_view>
#include <codecvt>
#include <locale>
#include <chrono>
#include <thread>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    // constexpr uint16_t domainMaxLength { 256 };
    constexpr uint16_t labelMaxLength { 64 };
    constexpr std::array<char, 4> acePrefix { 'x', 'n', '-', '-'};
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter {};

    template <uint16_t A, uint16_t B, class... Args>
    constexpr bool inRange(const Args &... param) noexcept
    {
        return ((static_cast<char>(param) >= A && B >= static_cast<char>(param)) && ...);
    }

    template<typename... Args>
    void insertToString(UInt8 * str, size_t & start, Args && ... args)
    {
        ((str[start++] = args), ...);
    }

    constexpr bool isASCII(const uint32_t u32) noexcept
    {
        if (u32 <= 0x7F) // 0XXXXXXX
        {
            return inRange<0, 127>(u32);
        }
        else if (u32 <= 0x7FF) // 110XXXXX 10XXXXXX
        {
            return inRange<0, 127>((0xC0 | ((u32 >> 6) & 0x1F)), (0x80 | (u32 & 0x3F)));
        }
        else if (u32 <= 0xFFFF) // 1110XXXX 10XXXXXX 10XXXXXX
        {
            return inRange<0, 127>((0xE0 | ((u32 >> 12) & 0x0F)), (0x80 | ((u32 >> 6) & 0x3F)),
                                   (0x80 | (u32 & 0x3F)));
        }
        else if (u32 <= 0x13FFFF)  // 11110XXX 10XXXXXX 10XXXXXX 10XXXXXX
        {
            return inRange<0, 127>((0xF0 | ((u32 >> 18) & 0x07)), (0x80 | ((u32 >> 12) & 0x3F)),
                                   (0x80 | ((u32 >> 6) & 0x3F)), (0x80 | (u32 & 0x3F)));
        }
        return false;
    }

    bool isASCIIStrUTF32(const std::u32string & input, size_t from, size_t until) noexcept
    {
        for (size_t i = from, size = std::min(input.size(), until); i < size; ++i)
        {
            if (!isASCII(input[i]))
                return false;
        }
        return true;
    }

    bool isEncoded(const std::string_view input, size_t start) noexcept
    {
        return (input.length() >= (start + 4)) && (input[start] == 'x' ||input[start] == 'X') &&
            (input[start + 1] == 'n' || input[start + 1] == 'N') &&
            (input[start + 2] == '-' || input[start + 4] == '-');
    };

    size_t appendUTF32ToString(const char32_t * input,
                               const size_t start, const size_t end,
                               UInt8 * decoded_string) noexcept
    {
        size_t pos {0};
        for (size_t i = start; i < end; ++i)
        {
            const uint32_t u32 = input[i];
            if (u32 <= 0x7F)
            {
                insertToString(decoded_string, pos, u32);
            }
            else if (u32 <= 0x7FF)
            {
                insertToString(decoded_string, pos, (0xC0 | ((u32 >> 6) & 0x1F)), (0x80 | (u32 & 0x3F)));
            }
            else if (u32 <= 0xFFFF)
            {
                insertToString(decoded_string, pos, (0xE0 | ((u32 >> 12) & 0x0F)),
                               (0x80 | ((u32 >> 6) & 0x3F)), (0x80 | (u32 & 0x3F)));
            }
            else if (u32 <= 0x13FFFF)
            {
                insertToString(decoded_string, pos,(0xF0 | ((u32 >> 18) & 0x07)), (0x80 | ((u32 >> 12) & 0x3F)),
                               (0x80 | ((u32 >> 6) & 0x3F)),(0x80 | (u32 & 0x3F)));
            }
        }
        return pos;
    }

    size_t punycodeEncodeInternal(const std::u32string & input,
                                  UInt8 * encoded_string) noexcept
    {
        char buf[labelMaxLength]{};
        size_t prev{0}, length{0}, encoded_length {0};
        for (size_t size = input.size(), idx = 0; idx < size; ++idx)
        {
            if ('.' == input[idx])
            {
                if (!isASCIIStrUTF32(input, prev, idx))
                {
                    const auto result_code = punycode_encode(idx - prev, input.data() + prev,
                                                             nullptr, &(length = labelMaxLength), buf);
                    if (punycode_success != result_code)
                        return 0;

                    std::copy_n(acePrefix.data(), acePrefix.size(), encoded_string + encoded_length);
                    encoded_length += acePrefix.size();
                    std::copy_n(buf, length, encoded_string + encoded_length);
                    encoded_length += length;
                }
                else
                {
                    encoded_length += appendUTF32ToString(input.data(), prev, idx, encoded_string + encoded_length);
                }

                encoded_string[encoded_length++] = '.';
                // To skip '.' with prefix increment '++idx'
                prev = ++idx;
            }
        }

        if (!isASCIIStrUTF32(input, prev, input.size()))
        {
            const auto result_code = punycode_encode(input.length() - prev, input.data() + prev,
                                                     nullptr, &(length = labelMaxLength), buf);
            if (punycode_success != result_code)
                return 0;

            std::copy_n(acePrefix.data(), acePrefix.size(), encoded_string + encoded_length);
            encoded_length += acePrefix.size();
            std::copy_n(buf, length, encoded_string + encoded_length);
            encoded_length += length;
        }
        else
        {
            encoded_length += appendUTF32ToString(input.data(), prev, input.size(), encoded_string + encoded_length);
        }
        return encoded_length;
    }

    /** If the string contains ' :// ' then this string contains the full DNS name
     *  if the string do not contains ' :// ' then we consider the entire input
     *  as data to be encoded regardless of the content of characters in it ' / '
     */
    constexpr std::pair<size_t, size_t> getFqdnBounds(std::string_view input) noexcept{
        std::string::size_type start = input.find("://");
        if (std::string::npos == start)
            return {0, input.size() };
        std::string::size_type last = input.find('/', start + 3);
        return {start + 3, std::string::npos == last ? input.size() : last};
    };

    size_t punycodeEncode(const std::string_view input,
                          UInt8 * encoded_string)
    {
        const auto [start, last] = getFqdnBounds(input);
        std::copy_n(input.data(), start, encoded_string);

        auto s32 = converter.from_bytes((input.data() + start), (input.data() + last));
        std::for_each(s32.begin(), s32.end(), [](auto & ch) { ch = Poco::Unicode::toLower(ch);});
        size_t decoded_length = punycodeEncodeInternal(s32, encoded_string + start);
        if (0 == decoded_length)
            throw std::runtime_error("Failed to encode");
        decoded_length += start;
        if (input.size() > last)
            encoded_string[decoded_length++] = '/';
        return decoded_length;
    }

    size_t punycodeDecode(const std::string_view input,
                          UInt8 * decoded_string)
    {
        const auto [start, last] = getFqdnBounds(input);
        std::copy_n(input.data(), start, decoded_string);

        char32_t utf32_buffer[labelMaxLength]{};
        size_t prev{start}, length{0}, decoded_length {start};
        for (size_t size = last, idx = start; idx < size; ++idx)
        {
            if ('.' == input[idx])
            {
                if (isEncoded(input, prev))
                {
                    prev += 4;
                    const auto result_code = punycode_decode(idx - prev, input.data() + prev,
                                                             &(length = labelMaxLength),
                                                             utf32_buffer, nullptr);
                    if (punycode_success != result_code)
                        throw std::runtime_error("Failed to decode");
                    decoded_length += appendUTF32ToString(utf32_buffer, 0, length, decoded_string + decoded_length);
                }
                else
                {
                    std::copy_n(input.data() + prev, idx - prev, decoded_string + decoded_length);
                    decoded_length += idx - prev;
                }
                prev = ++idx; // Skip '.' with prefix increment
                decoded_string[decoded_length++] = '.';
            }
        }

        if (isEncoded(input, prev))
        {
            prev += 4;
            const auto result_code = punycode_decode(last - prev, input.data() + prev,
                                                     &(length = labelMaxLength), utf32_buffer, nullptr);
            if (punycode_success != result_code)
                throw std::runtime_error("Failed to decode");
            decoded_length += appendUTF32ToString(utf32_buffer, 0, length, decoded_string + decoded_length);
        }
        else
        {
            std::copy_n(input.data() + prev, last - prev, decoded_string + decoded_length);
            decoded_length += last - prev;
        }

        if (input.size() > last)
            decoded_string[decoded_length++] = '/';
        return decoded_length;
    }

    struct PunycodeDecode
    {
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            if (data.empty())
                return;
            const size_t size = offsets.size();
            res_data.resize(data.size());
            res_offsets.resize(size);

            size_t bytes_written { 0 };
            for (size_t i = 0, from = 0; i < size; ++i)
            {
                const size_t length = offsets[i] - from - 1;
                const std::string_view data_view(reinterpret_cast<const char *>(&data[from]), length);

                bytes_written += punycodeDecode(data_view, res_data.data() + bytes_written);
                res_data[bytes_written++] = 0;

                res_offsets[i] = bytes_written;
                from = offsets[i];
            }
            res_data.resize(bytes_written);
        }

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
        {
            throw Exception("Unsupported operation", ErrorCodes::BAD_ARGUMENTS);
        }
    };

    struct PunycodeEncode
    {
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            if (data.empty())
                return;
            const size_t size = offsets.size();
            res_data.resize(data.size() * 2);
            res_offsets.resize(size);

            size_t bytes_written { 0 };
            for (size_t i = 0, from = 0; i < size; ++i)
            {
                const size_t length = offsets[i] - from - 1;
                const std::string_view data_view(reinterpret_cast<const char *>(&data[from]), length);

                bytes_written += punycodeEncode(data_view, res_data.data() + bytes_written);
                res_data[bytes_written++] = 0;

                res_offsets[i] = bytes_written;
                from = offsets[i];
            }
            res_data.resize(bytes_written);
        }

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
        {
            throw Exception("Unsupported operation", ErrorCodes::BAD_ARGUMENTS);
        }
    };

    struct NamePunycodeEncode { static constexpr auto name = "punycodeEncode"; };
    using FunctionPunycodeEncode = FunctionStringToString<PunycodeEncode, NamePunycodeEncode>;

    struct NamePunycodeDecode { static constexpr auto name = "punycodeDecode"; };
    using FunctionPunycodeDecode = FunctionStringToString<PunycodeDecode, NamePunycodeDecode>;

}

REGISTER_FUNCTION(Punycode)
{
    factory.registerFunction<FunctionPunycodeEncode>();
    factory.registerFunction<FunctionPunycodeDecode>();
}

}
