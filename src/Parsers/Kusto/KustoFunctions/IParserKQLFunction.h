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
    struct IncreaseDepthTag {};
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
    bool convert(String &out,IParser::Pos &pos);
    virtual const char * getName() const = 0;
    virtual ~IParserKQLFunction() = default;
protected:
    virtual bool convertImpl(String &out,IParser::Pos &pos) = 0;
    static bool directMapping(String &out,IParser::Pos &pos,const String &ch_fn);
    static String getConvertedArgument(const String &fn_name, IParser::Pos &pos);
    static String getKQLFunctionName(IParser::Pos &pos);
};

}

