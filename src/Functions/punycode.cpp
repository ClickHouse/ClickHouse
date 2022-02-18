#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include "Poco/Unicode.h"

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
    using punycode_uint = char32_t;
    constexpr punycode_uint maxint = -1;

    enum
    {
        punycode_success = 0,
        punycode_overflow = -1,
        punycode_big_output = -2,
        punycode_bad_input = -3
    };

    enum
    {
        base = 36,
        tmin = 1,
        tmax = 26,
        skew = 38,
        damp = 700,
        initial_bias = 72,
        initial_n = 0x80,
        delimiter = 0x2D
    };

    char encodeDigit(punycode_uint d, int flag)
    {
        return d + 22 + 75 * (d < 26) - ((flag != 0) << 5);
        /* 0 ..25 map to ASCII a..z or A..Z */
        /* 26..35 map to ASCII 0..9         */
    }

    unsigned int decodeDigit(int cp)
    {
        return static_cast<unsigned int>((cp - 48 < 10 ? cp - 22 :  cp - 65 < 26 ?
                                          cp - 65 : cp - 97 < 26 ? cp - 97 :  base));
    }

    punycode_uint adapt(punycode_uint delta,
                        punycode_uint numpoints,
                        bool firstTime)
    {
        punycode_uint k;
        delta = firstTime ? delta / damp : delta >> 1; /* delta >> 1 is a faster way of doing delta / 2 */
        delta += delta / numpoints;

        for (k = 0;  delta > ((base - tmin) * tmax) / 2;  k += base)
            delta /= base - tmin;
        return k + (base - tmin + 1) * delta / (delta + skew);
    }

    /** The punycode_unicode() and punycode_encode() functions are a simple copy-paste
     *  of pure C version of the implementation with minimalistic modifications or
     *  refactoring.
     *  https://datatracker.ietf.org/doc/html/rfc3492#appendix-C
     */
    int punycode_encode(const punycode_uint input[],
                        size_t input_length_orig,
                        char * const output,
                        size_t * output_length)
    {
        /* The Punycode spec assumes that the input length is the same type */
        /* of integer as a code point, so we need to convert the size_t to  */
        /* a punycode_uint, which could overflow.                           */
        if (input_length_orig > maxint)
            return punycode_overflow;

        punycode_uint n = initial_n, delta {0}, h, b, bias = initial_bias, j, m, q, k, t;
        size_t out {0}, max_out = *output_length;
        auto input_length = static_cast<punycode_uint>(input_length_orig);

        /* Handle the basic code points: */
        for (j = 0;  j < input_length;  ++j)
        {   /* basic(cp) tests whether cp is a basic code point: */
            if (static_cast<punycode_uint>(input[j]) < 0x80)
            {
                if (max_out - out < 2)
                    return punycode_big_output;
                output[out++] = static_cast<char>(input[j]);
            }
            /* else if (input[j] < n) return punycode_bad_input; */
            /* (not needed for Punycode with unsigned code points) */
        }

        h = b = static_cast<punycode_uint>(out);
        /* cannot overflow because out <= input_length <= maxint */

        /* h is the number of code points that have been handled, b is the  */
        /* number of basic code points, and out is the number of ASCII code */
        /* points that have been output.                                    */

        if (b > 0) output[out++] = delimiter;

        /* Main encoding loop: */
        while (h < input_length)
        {
            /* All non-basic code points < n have been     */
            /* handled already.  Find the next larger one: */

            for (m = maxint, j = 0;  j < input_length;  ++j)
            {
                /* if (basic(input[j])) continue; */
                /* (not needed for Punycode) */
                if (input[j] >= n && input[j] < m) m = input[j];
            }

            /* Increase delta enough to advance the decoder's    */
            /* <n,i> state to <m,0>, but guard against overflow: */

            if (m - n > (maxint - delta) / (h + 1)) return punycode_overflow;
            delta += (m - n) * (h + 1);
            n = m;

            for (j = 0;  j < input_length;  ++j)
            {
                /* Punycode does not need to check whether input[j] is basic: */
                if (input[j] < n)
                {
                    if (++delta == 0) return punycode_overflow;
                }

                if (input[j] == n)
                {
                    /* Represent delta as a generalized variable-length integer: */

                    for (q = delta, k = base;  ;  k += base)
                    {
                        if (out >= max_out) return punycode_big_output;
                        t = k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
                            k >= bias + tmax ? tmax : k - bias;
                        if (q < t) break;
                        output[out++] = encodeDigit(t + (q - t) % (base - t), 0);
                        q = (q - t) / (base - t);
                    }

                    output[out++] = encodeDigit(q, 0);
                    bias = adapt(delta, h + 1, h == b);
                    delta = 0;
                    ++h;
                }
            }
            ++delta;
            ++n;
        }

        *output_length = out;
        return punycode_success;
    }

    int punycode_decode(const char input[],
                        size_t input_length,
                        punycode_uint output[],
                        size_t * output_length)
    {
        punycode_uint out = 0;
        punycode_uint max_out = *output_length > maxint ? maxint : static_cast<punycode_uint>(*output_length);

        /* Handle the basic code points:  Let b be the number of input code */
        /* points before the last delimiter, or 0 if there is none, then    */
        /* copy the first b code points to the output.                      */
        size_t b;
        for (size_t j = b = 0;  j < input_length;  ++j)
        {
            if (delimiter == input[j]) b = j;
        }
        if (b > max_out) return punycode_big_output;

        for (size_t j = 0;  j < b;  ++j)
        {
            output[out++] = input[j];
        }

        punycode_uint n = initial_n, i = 0, bias = initial_bias, oldi {0}, w, k, digit, t;

        /* Main decoding loop:  Start just after the last delimiter if any  */
        /* basic code points were copied; start at the beginning otherwise. */
        for (size_t in = b > 0 ? b + 1 : 0;  in < input_length;  ++out)
        {
            /* in is the index of the next ASCII code point to be consumed, */
            /* and out is the number of code points in the output array.    */

            /* Decode a generalized variable-length integer into delta,  */
            /* which gets added to i.  The overflow checking is easier   */
            /* if we increase i as we go, then subtract off its starting */
            /* value at the end to obtain delta.                         */

            for (oldi = i, w = 1, k = base;  ;  k += base)
            {
                if (in >= input_length) return punycode_bad_input;
                digit = decodeDigit(input[in++]);
                if (digit >= base) return punycode_bad_input;
                if (digit > (maxint - i) / w) return punycode_overflow;
                i += digit * w;
                t = k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
                    k >= bias + tmax ? tmax : k - bias;
                if (digit < t) break;
                if (w > maxint / (base - t)) return punycode_overflow;
                w *= (base - t);
            }

            bias = adapt(i - oldi, out + 1, oldi == 0);

            /* i was supposed to wrap around from out+1 to 0,   */
            /* incrementing n each time, so we'll fix that now: */

            if (i / (out + 1) > maxint - n) return punycode_overflow;
            n += i / (out + 1);
            i %= (out + 1);

            /* Insert n at position i of the output: */

            /* not needed for Punycode: */
            /* if (basic(n)) return punycode_bad_input; */
            if (out >= max_out) return punycode_big_output;

            std::memmove(output + i + 1, output + i, (out - i) * sizeof *output);
            output[i++] = n;
        }

        *output_length = static_cast<size_t>(out);
        /* cannot overflow because out <= old value of *output_length */
        return punycode_success;
    }

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

    size_t appendUTF32ToString(const punycode_uint input[],
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
                    const auto result_code = punycode_encode(input.data() + prev, idx - prev,
                                                             buf, &(length = labelMaxLength));
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
            const auto result_code = punycode_encode(input.data() + prev, input.length() - prev,
                                                     buf, &(length = labelMaxLength));
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

        punycode_uint utf32_buffer[labelMaxLength]{};
        size_t prev{start}, length{0}, decoded_length {start};
        for (size_t size = last, idx = start; idx < size; ++idx)
        {
            if ('.' == input[idx])
            {
                if (isEncoded(input, prev))
                {
                    prev += 4;
                    const auto result_code = punycode_decode(input.data() + prev, idx - prev,
                                                             utf32_buffer, &(length = labelMaxLength));
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
            const auto result_code = punycode_decode(input.data() + prev, last - prev,
                                                     utf32_buffer, &(length = labelMaxLength));
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
