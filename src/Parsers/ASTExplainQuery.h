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
        QueryPipeline, /// 'EXPLAIN PIPELINE ...'
    };

    explicit ASTExplainQuery(ExplainKind kind_) : kind(kind_) {}

    String getID(char delim) const override { return "Explain" + (delim + toString(kind)); }
    ExplainKind getKind() const { return kind; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExplainQuery>(*this);
        res->children.clear();
        res->children.push_back(children[0]->clone());
        cloneOutputOptions(*res);
        return res;
    }

    void setExplainedQuery(ASTPtr query_)
    {
        children.emplace_back(query_);
        query = std::move(query_);
    }

    void setSettings(ASTPtr settings_)
    {
        children.emplace_back(settings_);
        ast_settings = std::move(settings_);
    }

    const ASTPtr & getExplainedQuery() const { return query; }
    const ASTPtr & getSettings() const { return ast_settings; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind) << (settings.hilite ? hilite_none : "");

        if (ast_settings)
        {
            settings.ostr << ' ';
            ast_settings->formatImpl(settings, state, frame);
        }

        settings.ostr << settings.nl_or_ws;
        query->formatImpl(settings, state, frame);
    }

private:
    ExplainKind kind;

    ASTPtr query;
    ASTPtr ast_settings;

    static String toString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "EXPLAIN AST";
            case AnalyzedSyntax: return "EXPLAIN SYNTAX";
            case QueryPlan: return "EXPLAIN";
            case QueryPipeline: return "EXPLAIN PIPELINE";
        }

        __builtin_unreachable();
    }
};

}
