#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearchToString.h>
#include <base/find_symbols.h>

namespace DB
{

struct ExtractURLParameterImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        std::string pattern,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            ColumnString::Offset cur_offset = offsets[i];

            const char * str = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = reinterpret_cast<const char *>(&data[cur_offset]);

            /// Find query string or fragment identifier.
            /// Note that we support parameters in fragment identifier in the same way as in query string.

            const char * const query_string_begin = find_first_symbols<'?', '#'>(str, end);

            /// Will point to the beginning of "name=value" pair. Then it will be reassigned to the beginning of "value".
            const char * param_begin = nullptr;

            if (query_string_begin + 1 < end)
            {
                param_begin = query_string_begin + 1;

                while (true)
                {
                    param_begin = static_cast<const char *>(memmem(param_begin, end - param_begin, param_str, param_len));

                    if (!param_begin)
                        break;

                    if (param_begin[-1] != '?' && param_begin[-1] != '#' && param_begin[-1] != '&')
                    {
                        /// Parameter name is different but has the same suffix.
                        param_begin += param_len;
                        continue;
                    }
                    else
                    {
                        param_begin += param_len;
                        break;
                    }
                }
            }

            if (param_begin)
            {
                const char * param_end = find_first_symbols<'&', '#'>(param_begin, end);
                if (param_end == end)
                    param_end = param_begin + strlen(param_begin);

                size_t param_size = param_end - param_begin;

                res_data.resize(res_offset + param_size + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], param_begin, param_size);
                res_offset += param_size;
            }
            else
            {
                /// No parameter found, put empty string in result.
                res_data.resize(res_offset + 1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};

struct NameExtractURLParameter { static constexpr auto name = "extractURLParameter"; };
using FunctionExtractURLParameter = FunctionsStringSearchToString<ExtractURLParameterImpl, NameExtractURLParameter>;

void registerFunctionExtractURLParameter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractURLParameter>();
}

}
