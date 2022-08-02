#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>

namespace DB
{
class IParserKQLFunction
{
public:
    template <typename F>
    ALWAYS_INLINE static bool wrapConvertImpl(IParser::Pos & pos, const F & func)
    {
        IParser::Pos begin = pos;
        bool res = func();
        if (!res)
            pos = begin;
        return res;
    }

    struct IncreaseDepthTag
    {
    };

    template <typename F>
    ALWAYS_INLINE static bool wrapConvertImpl(IParser::Pos & pos, IncreaseDepthTag, const F & func)
    {
        IParser::Pos begin = pos;
        pos.increaseDepth();
        bool res = func();
        pos.decreaseDepth();
        if (!res)
            pos = begin;
        return res;
    }

    bool convert(String & out, IParser::Pos & pos);
    virtual const char * getName() const = 0;
    virtual ~IParserKQLFunction() = default;

    static String getExpression(IParser::Pos & pos);

protected:
    virtual bool convertImpl(String & out, IParser::Pos & pos) = 0;

    static bool directMapping(String & out, IParser::Pos & pos, const String & ch_fn);
    static String getArgument(const String & function_name, DB::IParser::Pos & pos);
    static String getConvertedArgument(const String & fn_name, IParser::Pos & pos);
    static std::optional<String> getOptionalArgument(const String & function_name, DB::IParser::Pos & pos);
    static String kqlCallToExpression(const String & function_name, std::initializer_list<std::string_view> params, uint32_t max_depth);
    static void validateEndOfFunction(const String & fn_name, IParser::Pos & pos);
    static String getKQLFunctionName(IParser::Pos & pos);
};
}
