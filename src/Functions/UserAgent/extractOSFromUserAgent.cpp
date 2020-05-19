#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearchToString.h>

namespace DB
{

struct ExtractOSFromUserAgentImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        std::string pattern,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        pattern += '=';

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);

            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, 3);
            res_offset += 4;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};

struct NameExtractOSFromUserAgent { static constexpr auto name = "extractOSFromUserAgent"; };
using FunctionExtractOSFromUserAgent = FunctionsStringSearchToString<ExtractOSFromUserAgentImpl, NameExtractOSFromUserAgent>;

void registerFunctionExtractOSFromUserAgent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractOSFromUserAgent>();
}

}
