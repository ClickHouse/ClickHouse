#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/// AST, EXPLAIN or other query with meaning of explanation query instead of execution
class ASTExplainQuery : public ASTQueryWithOutput
{
public:
    enum ExplainKind
    {
        ParsedAST, /// 'EXPLAIN AST SELECT ...'
        AnalyzedSyntax, /// 'EXPLAIN SYNTAX SELECT ...'
        QueryPlan, /// 'EXPLAIN SELECT ...'
    };

    ASTExplainQuery(ExplainKind kind_, bool old_syntax_)
        : kind(kind_), old_syntax(old_syntax_)
    {
        children.emplace_back(); /// explained query
    }

    String getID(char delim) const override { return "Explain" + (delim + toString(kind, old_syntax)); }
    ExplainKind getKind() const { return kind; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExplainQuery>(*this);
        res->children.clear();
        res->children.push_back(children[0]->clone());
        cloneOutputOptions(*res);
        return res;
    }

    ASTPtr & getExplainedQuery() { return children.at(0); }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind, old_syntax) << (settings.hilite ? hilite_none : "") << " ";
        children.at(0)->formatImpl(settings, state, frame);
    }

private:
    ExplainKind kind;
    bool old_syntax; /// "EXPLAIN AST" -> "AST", "EXPLAIN SYNTAX" -> "ANALYZE"

    static String toString(ExplainKind kind, bool old_syntax)
    {
        switch (kind)
        {
            case ParsedAST: return old_syntax ? "AST" : "EXPLAIN AST";
            case AnalyzedSyntax: return old_syntax ? "ANALYZE" : "EXPLAIN SYNTAX";
            case QueryPlan: return "EXPLAIN";
        }

        __builtin_unreachable();
    }
};

}
