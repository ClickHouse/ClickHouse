#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/find_symbols.h>

#include <vector>
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    struct DecodeXMLComponentName
    {
        static constexpr auto name = "decodeXMLComponent";
    };

    class FunctionDecodeXMLComponentImpl
    {
    public:
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            res_data.resize(data.size());
            size_t size = offsets.size();
            res_offsets.resize(size);

            size_t prev_offset = 0;
            size_t res_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
                size_t src_size = offsets[i] - prev_offset;
                size_t dst_size = execute(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset));

                res_offset += dst_size;
                res_offsets[i] = res_offset;
                prev_offset = offsets[i];
            }

            res_data.resize(res_offset);
        }

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
        {
            throw Exception("Function decodeXMLComponent cannot work with FixedString argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

    private:
        static const int max_legal_unicode_value = 0x10FFFF;
        static const int max_legal_unicode_bits = 7;
        static size_t execute(const char * src, size_t src_size, char * dst)
        {
            const char * src_prev_pos = src;
            const char * src_curr_pos = src;
            const char * src_next_pos = src;
            const char * src_end = src + src_size;
            char * dst_pos = dst;

            while (true)
            {
                src_curr_pos = find_first_symbols<'&'>(src_curr_pos, src_end);

                if (src_curr_pos == src_end)
                {
                    break;
                }
                else if (*src_curr_pos == '&')
                {
                    src_next_pos = find_first_symbols<';'>(src_curr_pos, src_end);
                    if (src_next_pos == src_end)
                    {
                        src_curr_pos = src_end;
                        break;
                    }
                    else if (isValidNumeric(src_curr_pos, src_next_pos))
                    {
                        std::vector<char> decodeNumericChars;
                        decodeNumericPart(src_curr_pos + 2, src_next_pos, decodeNumericChars);
                        if (decodeNumericChars.empty())
                        {
                            ++src_curr_pos;
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            src_prev_pos = src_curr_pos;
                        }
                        else
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            for (size_t i = 0; i < decodeNumericChars.size(); i++)
                            {
                                *dst_pos = decodeNumericChars[i];
                                ++dst_pos;
                            }
                            src_prev_pos = src_next_pos + 1;
                        }
                        src_curr_pos = src_next_pos + 1;
                    }
                    else if (src_next_pos - src_curr_pos == 3)
                    {
                        if (strncmp(src_curr_pos, "&lt", 3) == 0)
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            *dst_pos = '<';
                            ++dst_pos;
                            src_prev_pos = src_curr_pos + 4;
                        }
                        else if (strncmp(src_curr_pos, "&gt", 3) == 0)
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            *dst_pos = '>';
                            ++dst_pos;
                            src_prev_pos = src_curr_pos + 4;
                        }
                        else
                        {
                            ++src_curr_pos;
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            src_prev_pos = src_curr_pos;
                            continue;
                        }
                        src_curr_pos += 4;
                    }
                    else if (src_next_pos - src_curr_pos == 4)
                    {
                        if (strncmp(src_curr_pos, "&amp", 4) == 0)
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            *dst_pos = '&';
                            ++dst_pos;
                            src_prev_pos = src_curr_pos + 5;
                        }
                        else
                        {
                            ++src_curr_pos;
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            src_prev_pos = src_curr_pos;
                            continue;
                        }
                        src_curr_pos += 5;
                    }
                    else if (src_next_pos - src_curr_pos == 5)
                    {
                        if (strncmp(src_curr_pos, "&quot", 5) == 0)
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            *dst_pos = '"';
                            ++dst_pos;
                            src_prev_pos = src_curr_pos + 6;
                        }
                        else if (strncmp(src_curr_pos, "&apos", 5) == 0)
                        {
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            *dst_pos = '\'';
                            ++dst_pos;
                            src_prev_pos = src_curr_pos + 6;
                        }
                        else
                        {
                            ++src_curr_pos;
                            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                            dst_pos += bytes_to_copy;
                            src_prev_pos = src_curr_pos;
                            continue;
                        }
                        src_curr_pos += 6;
                    }
                    else
                    {
                        ++src_curr_pos;
                        size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                        memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                        dst_pos += bytes_to_copy;
                        src_prev_pos = src_curr_pos;
                    }
                }
            }

            if (src_prev_pos < src_curr_pos)
            {
                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;
            }

            return dst_pos - dst;
        }

        static void decodeNumericPart(const char * src, const char * end, std::vector<char> & decodeNumericChars)
        {
            int numeric_ans;
            if (*src == 'x' || *src == 'X')
            {
                numeric_ans = hexOrDecStrToInt(src + 1, end, 16);
            }
            else
            {
                numeric_ans = hexOrDecStrToInt(src, end, 10);
            }
            const auto num_bits = numBitsCount(numeric_ans);
            if (num_bits <= 7)
            {
                decodeNumericChars.push_back('\0' + (numeric_ans & 0x7F));
            }
            else if (num_bits <= 11)
            {
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 6) & 0x1F) + 0xC0);
                decodeNumericChars.push_back('\0' + (numeric_ans & 0x3F) + 0x80);
            }
            else if (num_bits <= 16)
            {
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 12) & 0x0F) + 0xE0);
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 6) & 0x3F) + 0x80);
                decodeNumericChars.push_back('\0' + (numeric_ans & 0x3F) + 0x80);
            }
            else if ((num_bits <= 21) && (numeric_ans <= max_legal_unicode_value))
            {
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 18) & 0x07) + 0xF0);
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 12) & 0x3F) + 0x80);
                decodeNumericChars.push_back('\0' + ((numeric_ans >> 6) & 0x3F) + 0x80);
                decodeNumericChars.push_back('\0' + (numeric_ans & 0x3F) + 0x80);
            }
        }

        static int hexOrDecStrToInt(const char * src, const char * end, int base)
        {
            int numeric_ans = 0;
            int pos = 0;
            if (base == 16)
            {
                while (src + pos != end)
                {
                    if (isNumericASCII(*(src + pos)))
                    {
                        numeric_ans = numeric_ans * base + (*(src + pos) - '0');
                    }
                    else if (*(src + pos) >= 'a' && *(src + pos) <= 'f')
                    {
                        numeric_ans = numeric_ans * base + (*(src + pos) - 'a' + 10);
                    }
                    else if (*(src + pos) >= 'A' && *(src + pos) <= 'F')
                    {
                        numeric_ans = numeric_ans * base + (*(src + pos) - 'A' + 10);
                    }
                    ++pos;
                }
            }
            else
            {
                while (src + pos != end)
                {
                    numeric_ans = numeric_ans * base + (*(src + pos) - '0');
                    ++pos;
                }
            }
            return numeric_ans;
        }
        static int numBitsCount(int integer)
        {
            size_t num_bits = 0;
            while (integer > 0)
            {
                ++num_bits;
                integer >>= 1;
            }
            return num_bits;
        }
        static bool isValidNumeric(const char * src, const char * end)
        {
            int pos;
            if (*src != '&' || *(src + 1) != '#' || (end - (src + 2) > max_legal_unicode_bits))
            {
                return false;
            }
            if (*(src + 2) == 'x' || *(src + 2) == 'X')
            {
                pos = 3;
                while (src + pos != end)
                {
                    if (!isHexDigit(*(src + pos)))
                    {
                        return false;
                    }
                    ++pos;
                }
                return true;
            }
            else
            {
                pos = 2;
                while (src + pos != end)
                {
                    if (!isNumericASCII(*(src + pos)))
                    {
                        return false;
                    }
                    ++pos;
                }
                return true;
            }
        }
    };

    using FunctionDecodeXMLComponent = FunctionStringToString<FunctionDecodeXMLComponentImpl, DecodeXMLComponentName>;

}

void registerFunctionDecodeXMLComponent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDecodeXMLComponent>();
}
}
