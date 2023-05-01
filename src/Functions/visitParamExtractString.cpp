#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearchToString.h>


namespace DB
{

struct ExtractString
{
    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars & res_data)
    {
        size_t old_size = res_data.size();
        ReadBufferFromMemory in(pos, end - pos);
        if (!tryReadJSONStringInto(res_data, in))
            res_data.resize(old_size);
    }
};

struct NameSimpleJSONExtractString { static constexpr auto name = "simpleJSONExtractString"; };
using FunctionSimpleJSONExtractString = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractString>, NameSimpleJSONExtractString>;

REGISTER_FUNCTION(VisitParamExtractString)
{
    factory.registerFunction<FunctionSimpleJSONExtractString>();
    factory.registerAlias("visitParamExtractString", "simpleJSONExtractString");
}

}
