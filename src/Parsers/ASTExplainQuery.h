#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// AST, EXPLAIN or other query with meaning of explanation query instead of execution
class ASTExplainQuery : public ASTQueryWithOutput
{
public:
    enum ExplainKind
    {
        ParsedAST, /// 'EXPLAIN AST SELECT ...'
        AnalyzedSyntax, /// 'EXPLAIN SYNTAX SELECT ...'
        QueryTree, /// 'EXPLAIN QUERY TREE SELECT ...'
        QueryPlan, /// 'EXPLAIN SELECT ...'
        QueryPipeline, /// 'EXPLAIN PIPELINE ...'
        QueryEstimates, /// 'EXPLAIN ESTIMATE ...'
        TableOverride, /// 'EXPLAIN TABLE OVERRIDE ...'
        CurrentTransaction, /// 'EXPLAIN CURRENT TRANSACTION'
    };

    static String toString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "EXPLAIN AST";
            case AnalyzedSyntax: return "EXPLAIN SYNTAX";
            case QueryTree: return "EXPLAIN QUERY TREE";
            case QueryPlan: return "EXPLAIN";
            case QueryPipeline: return "EXPLAIN PIPELINE";
            case QueryEstimates: return "EXPLAIN ESTIMATE";
            case TableOverride: return "EXPLAIN TABLE OVERRIDE";
            case CurrentTransaction: return "EXPLAIN CURRENT TRANSACTION";
        }
    }

    static ExplainKind fromString(const String & str)
    {
        if (str == "EXPLAIN AST")
            return ParsedAST;
        if (str == "EXPLAIN SYNTAX")
            return AnalyzedSyntax;
        if (str == "EXPLAIN QUERY TREE")
            return QueryTree;
        if (str == "EXPLAIN" || str == "EXPLAIN PLAN")
            return QueryPlan;
        if (str == "EXPLAIN PIPELINE")
            return QueryPipeline;
        if (str == "EXPLAIN ESTIMATE")
            return QueryEstimates;
        if (str == "EXPLAIN TABLE OVERRIDE")
            return TableOverride;
        if (str == "EXPLAIN CURRENT TRANSACTION")
            return CurrentTransaction;

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown explain kind '{}'", str);
    }

    explicit ASTExplainQuery(ExplainKind kind_) : kind(kind_) {}

    String getID(char delim) const override { return "Explain" + (delim + toString(kind)); }
    ExplainKind getKind() const { return kind; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExplainQuery>(*this);
        res->children.clear();
        if (!children.empty())
            res->children.push_back(children[0]->clone());
        cloneOutputOptions(*res);
        return res;
    }

    void setExplainKind(ExplainKind kind_) { kind = kind_; }

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

    void setTableFunction(ASTPtr table_function_)
    {
        children.emplace_back(table_function_);
        table_function = std::move(table_function_);
    }

    void setTableOverride(ASTPtr table_override_)
    {
        children.emplace_back(table_override_);
        table_override = std::move(table_override_);
    }

    const ASTPtr & getExplainedQuery() const { return query; }
    const ASTPtr & getSettings() const { return ast_settings; }
    const ASTPtr & getTableFunction() const { return table_function; }
    const ASTPtr & getTableOverride() const { return table_override; }

    QueryKind getQueryKind() const override { return QueryKind::Explain; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(kind) << (settings.hilite ? hilite_none : "");

        if (ast_settings)
        {
            settings.ostr << ' ';
            ast_settings->formatImpl(settings, state, frame);
        }

        if (query)
        {
            settings.ostr << settings.nl_or_ws;
            query->formatImpl(settings, state, frame);
        }
        if (table_function)
        {
            settings.ostr << settings.nl_or_ws;
            table_function->formatImpl(settings, state, frame);
        }
        if (table_override)
        {
            settings.ostr << settings.nl_or_ws;
            table_override->formatImpl(settings, state, frame);
        }
    }

private:
    ExplainKind kind;

    ASTPtr query;
    ASTPtr ast_settings;

    /// Used by EXPLAIN TABLE OVERRIDE
    ASTPtr table_function;
    ASTPtr table_override;
};

}
