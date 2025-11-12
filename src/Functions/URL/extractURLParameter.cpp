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
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(input_rows_count);

        pattern += '=';
        const char * param_str = pattern.data();
        size_t param_len = pattern.size();

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
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

                    char prev_char = param_begin[-1];
                    if (prev_char != '?' && prev_char != '#' && prev_char != '&')
                    {
                        /// Parameter name is different but has the same suffix.
                        param_begin += param_len;
                        continue;
                    }

                    param_begin += param_len;
                    break;
                }
            }

            if (param_begin)
            {
                const char * param_end = find_first_symbols<'&', '#'>(param_begin, end);
                size_t param_size = param_end - param_begin;

                res_data.resize(res_offset + param_size);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], param_begin, param_size);
                res_offset += param_size;
            }

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }
};

struct NameExtractURLParameter { static constexpr auto name = "extractURLParameter"; };
using FunctionExtractURLParameter = FunctionsStringSearchToString<ExtractURLParameterImpl, NameExtractURLParameter>;

REGISTER_FUNCTION(ExtractURLParameter)
{
    /// extractURLParameter documentation
    FunctionDocumentation::Description description_extractURLParameter = R"(
Returns the value of the `name` parameter in the URL, if present, otherwise an empty string is returned.
If there are multiple parameters with this name, the first occurrence is returned.
The function assumes that the parameter in the `url` parameter is encoded in the same way as in the `name` argument.
    )";
    FunctionDocumentation::Syntax syntax_extractURLParameter = "extractURLParameter(url, name)";
    FunctionDocumentation::Arguments arguments_extractURLParameter = {
        {"url", "URL.", {"String"}},
        {"name", "Parameter name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_extractURLParameter = {"Returns the value of the URL parameter with the specified name.", {"String"}};
    FunctionDocumentation::Examples examples_extractURLParameter = {
    {
        "Usage example",
        R"(
SELECT extractURLParameter('http://example.com/?param1=value1&param2=value2', 'param1');
        )",
        R"(
┌─extractURLPa⋯, 'param1')─┐
│ value1                   │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_extractURLParameter = {1, 1};
    FunctionDocumentation::Category category_extractURLParameter = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_extractURLParameter = {description_extractURLParameter, syntax_extractURLParameter, arguments_extractURLParameter, returned_value_extractURLParameter, examples_extractURLParameter, introduced_in_extractURLParameter, category_extractURLParameter};

    factory.registerFunction<FunctionExtractURLParameter>(documentation_extractURLParameter);
}

}
