#pragma once

#include <Common/Documentation.h>
#include <Parsers/IParser.h>

#include <map>


namespace DB
{

/** Base class for most parsers
  */
class IParserBase : public IParser
{
public:
    template <typename F>
    ALWAYS_INLINE static bool wrapParseImpl(Pos & pos, const F & func)
    {
        Pos begin = pos;
        bool res = func();
        if (!res)
            pos = begin;
        return res;
    }

    struct IncreaseDepthTag {};

    template <typename F>
    ALWAYS_INLINE static bool wrapParseImpl(Pos & pos, IncreaseDepthTag, const F & func)
    {
        Pos begin = pos;
        pos.increaseDepth();
        bool res = func();
        pos.decreaseDepth();
        if (!res)
            pos = begin;
        return res;
    }

    bool parse(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// The embedded documentation of the SQL statements (or clauses) parsed by this parser.
    /// A parser can document more than one statement.
    /// Example: `ParserDropQuery` documents `DROP`, `DETACH` and `TRUNCATE`.
    virtual std::map<String, Documentation> getDocumentation() const { return {}; }

protected:
    virtual bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) = 0;
};

}
