#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <common/find_symbols.h>

#include <cstdio>
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
        static const int min_XML_number = 32;
        static const int max_XML_number = 126;
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
                        else if (*(src_curr_pos + 1) == '#' && isdigit(*(src_curr_pos + 2)) && isdigit(*(src_curr_pos + 3)))
                        {
                            char numeric_character = decodeNumberPart(src_curr_pos + 2);
                            if (numeric_character == '\0')
                            {
                                size_t bytes_to_copy = src_next_pos - src_prev_pos;
                                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                                dst_pos += bytes_to_copy;
                                src_prev_pos = src_curr_pos + 5;
                            }
                            else
                            {
                                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                                dst_pos += bytes_to_copy;
                                *dst_pos = '\0' + numeric_character;
                                ++dst_pos;
                                src_prev_pos = src_curr_pos + 5;
                            }
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
                        else if (
                            *(src_curr_pos + 1) == '#' && isdigit(*(src_curr_pos + 2)) && isdigit(*(src_curr_pos + 3))
                            && isdigit(*(src_curr_pos + 4)))
                        {
                            char numeric_character = decodeNumberPart(src_curr_pos + 2);
                            if (numeric_character == '\0')
                            {
                                size_t bytes_to_copy = src_next_pos - src_prev_pos;
                                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                                dst_pos += bytes_to_copy;
                                src_prev_pos = src_curr_pos + 6;
                            }
                            else
                            {
                                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                                dst_pos += bytes_to_copy;
                                *dst_pos = '\0' + numeric_character;
                                ++dst_pos;
                                src_prev_pos = src_curr_pos + 6;
                            }
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

        static inline char decodeNumberPart(const char * src)
        {
            auto numberic_ans = strtol(src, nullptr, 10);
            if (numberic_ans >= min_XML_number && numberic_ans <= max_XML_number)
            {
                return '\0' + numberic_ans;
            }
            return '\0';
        }
    };

    using FunctionDecodeXMLComponent = FunctionStringToString<FunctionDecodeXMLComponentImpl, DecodeXMLComponentName>;

}

void registerFunctionDecodeXMLComponent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDecodeXMLComponent>();
}
}
