#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/// AST, EXPLAIN or other query with meaning of explanation query instead of execution
class ASTExplainQuery : public IAST
{
public:
    enum ExplainKind
    {
        ParsedAST,
    };

    ASTExplainQuery(ExplainKind kind_ = ParsedAST)
        : kind(kind_)
    {}

    String getID() const override { return "Explain_" + toString(kind); }
    ASTPtr clone() const override { return std::make_shared<ASTExplainQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind) << (settings.hilite ? hilite_none : "");
    }

private:
    ExplainKind kind;

    static String toString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "ParsedAST";
        }
        __builtin_unreachable();
    }
};

}
