#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    struct EncodeXMLComponentName
    {
        static constexpr auto name = "encodeXMLComponent";
    };

    class FunctionEncodeXMLComponentImpl
    {
    public:
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            size_t input_rows_count)
        {
            /// 6 is the maximum size amplification (the maximum length of encoded entity: &quot;)
            res_data.resize(data.size() * 6);
            res_offsets.resize(input_rows_count);

            size_t prev_offset = 0;
            size_t res_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
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

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function encodeXML cannot work with FixedString argument");
        }

    private:
        static size_t execute(const char * src, size_t src_size, char * dst)
        {
            const char * src_prev_pos = src;
            const char * src_curr_pos = src;
            const char * src_end = src + src_size;
            char * dst_pos = dst;

            while (true)
            {
                src_curr_pos = find_first_symbols<'<', '&', '>', '"', '\''>(src_curr_pos, src_end);

                if (src_curr_pos == src_end)
                {
                    break;
                }
                if (*src_curr_pos == '<')
                {
                    size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                    memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                    dst_pos += bytes_to_copy;
                    memcpy(dst_pos, "&lt;", 4);
                    dst_pos += 4;
                    src_prev_pos = src_curr_pos + 1;
                    ++src_curr_pos;
                }
                else if (*src_curr_pos == '&')
                {
                    size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                    memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                    dst_pos += bytes_to_copy;
                    memcpy(dst_pos, "&amp;", 5);
                    dst_pos += 5;
                    src_prev_pos = src_curr_pos + 1;
                    ++src_curr_pos;
                }
                else if (*src_curr_pos == '>')
                {
                    size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                    memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                    dst_pos += bytes_to_copy;
                    memcpy(dst_pos, "&gt;", 4);
                    dst_pos += 4;
                    src_prev_pos = src_curr_pos + 1;
                    ++src_curr_pos;
                }
                else if (*src_curr_pos == '"')
                {
                    size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                    memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                    dst_pos += bytes_to_copy;
                    memcpy(dst_pos, "&quot;", 6);
                    dst_pos += 6;
                    src_prev_pos = src_curr_pos + 1;
                    ++src_curr_pos;
                }
                else if (*src_curr_pos == '\'')
                {
                    size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                    memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                    dst_pos += bytes_to_copy;
                    memcpy(dst_pos, "&apos;", 6);
                    dst_pos += 6;
                    src_prev_pos = src_curr_pos + 1;
                    ++src_curr_pos;
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
    };

    using FunctionEncodeXMLComponent = FunctionStringToString<FunctionEncodeXMLComponentImpl, EncodeXMLComponentName>;

}

REGISTER_FUNCTION(EncodeXMLComponent)
{
    factory.registerFunction<FunctionEncodeXMLComponent>();
}
}
