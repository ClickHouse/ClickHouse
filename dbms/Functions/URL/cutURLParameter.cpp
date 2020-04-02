#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearchToString.h>
#include <common/find_symbols.h>

namespace DB
{

struct CutURLParameterImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        std::string pattern,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * url_end = reinterpret_cast<const char *>(&data[cur_offset]) - 1;
            const char * begin_pos = url_begin;
            const char * end_pos = begin_pos;

            do
            {
                const char * query_string_begin = find_first_symbols<'?', '#'>(url_begin, url_end);
                if (query_string_begin + 1 >= url_end)
                    break;

                const char * pos = static_cast<const char *>(memmem(query_string_begin + 1, url_end - query_string_begin - 1, param_str, param_len));
                if (pos == nullptr)
                    break;

                if (pos[-1] != '?' && pos[-1] != '#' && pos[-1] != '&')
                {
                    pos = nullptr;
                    break;
                }

                begin_pos = pos;
                end_pos = begin_pos + param_len;

                /// Skip the value.
                while (*end_pos && *end_pos != '&' && *end_pos != '#')
                    ++end_pos;

                /// Capture '&' before or after the parameter.
                if (*end_pos == '&')
                    ++end_pos;
                else if (begin_pos[-1] == '&')
                    --begin_pos;
            } while (false);

            size_t cut_length = (url_end - url_begin) - (end_pos - begin_pos);
            res_data.resize(res_offset + cut_length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, begin_pos - url_begin);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset] + (begin_pos - url_begin), end_pos, url_end - end_pos);
            res_offset += cut_length + 1;
            res_data[res_offset - 1] = 0;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};

struct NameCutURLParameter { static constexpr auto name = "cutURLParameter"; };
using FunctionCutURLParameter = FunctionsStringSearchToString<CutURLParameterImpl, NameCutURLParameter>;

void registerFunctionCutURLParameter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutURLParameter>();
}

}
