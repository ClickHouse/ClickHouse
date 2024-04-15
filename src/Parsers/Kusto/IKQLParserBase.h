#pragma once
#include <Parsers/CommonParsers.h>
#include <Parsers/Kusto/IKQLParser.h>


namespace DB
{

/** Base class for most parsers
  */
class IKQLParserBase : public IKQLParser
{
public:
    template <typename F>
    ALWAYS_INLINE static bool wrapParseImpl(KQLPos & pos, const F & func)
    {
        KQLPos begin = pos;
        bool res = func();
        if (!res)
            pos = begin;
        return res;
    }

    struct IncreaseDepthTag {};

    template <typename F>
    ALWAYS_INLINE static bool wrapParseImpl(KQLPos & pos, IncreaseDepthTag, const F & func)
    {
        KQLPos begin = pos;
        pos.increaseDepth();
        bool res = func();
        pos.decreaseDepth();
        if (!res)
            pos = begin;
        return res;
    }

    bool parse(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;

protected:
    virtual bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) = 0;
};


class ParserKQLKeyword : public IKQLParserBase
{
private:
    std::string_view s;

    explicit ParserKQLKeyword(std::string_view s_): s(s_) { assert(!s.empty()); }

public:
    static ParserKQLKeyword createDeprecated(std::string_view s_)
    {
        return ParserKQLKeyword(s_);
    }

    static std::shared_ptr<ParserKQLKeyword> createDeprecatedPtr(std::string_view s_)
    {
        return std::shared_ptr<ParserKQLKeyword>(new ParserKQLKeyword(s_));
    }

    explicit ParserKQLKeyword(Keyword keyword);

    constexpr const char * getName() const override { return s.data(); }

protected:
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};


class ParserKQLToken : public IKQLParserBase
{
private:
    KQLTokenType token_type;
public:
    ParserKQLToken(KQLTokenType token_type_) : token_type(token_type_) {} /// NOLINT

protected:
    const char * getName() const override { return "kqltoken"; }

    bool parseImpl(KQLPos & pos, ASTPtr & /*node*/, KQLExpected & expected) override
    {
        if (pos->type != token_type)
        {
            expected.add(pos, getTokenName(token_type));
            return false;
        }
        ++pos;
        return true;
    }
};

}
