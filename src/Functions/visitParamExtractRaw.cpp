#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearchToString.h>


namespace DB
{

struct ExtractRaw
{
    using ExpectChars = PODArrayWithStackMemory<char, 64>;

    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars & res_data)
    {
        ExpectChars expects_end;
        UInt8 current_expect_end = 0;

        for (const auto * extract_begin = pos; pos != end; ++pos)
        {
            if (current_expect_end && *pos == current_expect_end)
            {
                expects_end.pop_back();
                current_expect_end = expects_end.empty() ? 0 : expects_end.back();
            }
            else if (current_expect_end == '"')
            {
                /// skip backslash
                if (*pos == '\\' && pos + 1 < end && pos[1] == '"')
                    ++pos;
            }
            else
            {
                switch (*pos)
                {
                    case '[':
                        current_expect_end = ']';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '{':
                        current_expect_end = '}';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '"' :
                        current_expect_end = '"';
                        expects_end.push_back(current_expect_end);
                        break;
                    default:
                        if (!current_expect_end && (*pos == ',' || *pos == '}'))
                        {
                            res_data.insert(extract_begin, pos);
                            return;
                        }
                }
            }
        }
    }
};

struct NameVisitParamExtractRaw    { static constexpr auto name = "visitParamExtractRaw"; };
using FunctionVisitParamExtractRaw = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractRaw>, NameVisitParamExtractRaw>;


void registerFunctionVisitParamExtractRaw(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractRaw>();
}

}
