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
        AnalyzedSyntax,
    };

    ASTExplainQuery(ExplainKind kind_)
        : kind(kind_)
    {}

    String getID(char delim) const override { return "Explain" + (delim + toString(kind)); }
    ExplainKind getKind() const { return kind; }
    ASTPtr clone() const override { return std::make_shared<ASTExplainQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind) << (settings.hilite ? hilite_none : "") << " ";
        children.at(0)->formatImpl(settings, state, frame);
    }

private:
    ExplainKind kind;

    static String toString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "AST";
            case AnalyzedSyntax: return "ANALYZE";
        }

        __builtin_unreachable();
    }
};

}
