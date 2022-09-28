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
        QueryPlan, /// 'EXPLAIN SELECT ...'
        QueryPipeline, /// 'EXPLAIN PIPELINE ...'
        QueryEstimates, /// 'EXPLAIN ESTIMATE ...'
        TableOverride, /// 'EXPLAIN TABLE OVERRIDE ...'
        CurrentTransaction, /// 'EXPLAIN CURRENT TRANSACTION'
    };

    explicit ASTExplainQuery(ExplainKind kind_) : kind(kind_) {}

    String getID(char delim) const override { return "Explain" + (delim + formatString(kind)); }
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

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << formatString(kind) << (settings.hilite ? hilite_none : "");

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

    /// format as it appears in the query text
    static String formatString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "EXPLAIN AST";
            case AnalyzedSyntax: return "EXPLAIN SYNTAX";
            case QueryPlan: return "EXPLAIN";
            case QueryPipeline: return "EXPLAIN PIPELINE";
            case QueryEstimates: return "EXPLAIN ESTIMATE";
            case TableOverride: return "EXPLAIN TABLE OVERRIDE";
            case CurrentTransaction: return "EXPLAIN CURRENT TRANSACTION";
        }

        __builtin_unreachable();
    }

public:
    static String kindToString(ExplainKind kind)
    {
        switch (kind)
        {
            case ParsedAST: return "AST";
            case AnalyzedSyntax: return "SYNTAX";
            case QueryTree: return "QUERY TREE";
            case QueryPlan: return "PLAN";
            case QueryPipeline: return "PIPELINE";
            case QueryEstimates: return "ESTIMATE";
            case TableOverride: return "TABLE OVERRIDE";
            case CurrentTransaction: return "CURRENT TRANSACTION";
        }

        __builtin_unreachable();
    }

    static ExplainKind kindFromString(const String & kind)
    {
        if (kind == "ast" || kind == "AST")
            return ExplainKind::ParsedAST;
        if (kind == "syntax" || kind == "SYNTAX")
            return ExplainKind::AnalyzedSyntax;
        if (kind == "query tree" || kind == "QUERY TREE")
            return ExplainKind::QueryTree;
        if (kind == "plan" || kind == "PLAN")
            return ExplainKind::QueryPlan;
        if (kind == "pipeline" || kind == "PIPELINE")
            return ExplainKind::QueryPipeline;
        if (kind == "estimate" || kind == "ESTIMATE")
            return ExplainKind::QueryEstimates;
        if (kind == "table override" || kind == "TABLE OVERRIDE")
            return ExplainKind::TableOverride;
        if (kind == "current transaction" || kind == "CURRENT TRANSACTION")
            return ExplainKind::CurrentTransaction;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal explain kind '{}'", kind);
    }
};


}
