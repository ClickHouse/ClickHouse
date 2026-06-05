#include <Parsers/ParserExplainQuery.h>

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSystemQuery.h>

namespace DB
{

bool ParserExplainQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTExplainQuery::ExplainKind kind = {};

    ParserKeyword s_ast(Keyword::AST);
    ParserKeyword s_explain(Keyword::EXPLAIN);
    ParserKeyword s_syntax(Keyword::SYNTAX);
    ParserKeyword s_query_tree(Keyword::QUERY_TREE);
    ParserKeyword s_pipeline(Keyword::PIPELINE);
    ParserKeyword s_plan(Keyword::PLAN);
    ParserKeyword s_estimates(Keyword::ESTIMATE);
    ParserKeyword s_table_override(Keyword::TABLE_OVERRIDE);
    ParserKeyword s_current_transaction(Keyword::CURRENT_TRANSACTION);

    if (s_explain.ignore(pos, expected))
    {
        kind = ASTExplainQuery::QueryPlan;

        if (s_ast.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::ParsedAST;
        else if (s_syntax.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::AnalyzedSyntax;
        else if (s_query_tree.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryTree;
        else if (s_pipeline.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPipeline;
        else if (s_plan.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPlan;
        else if (s_estimates.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryEstimates;
        else if (s_table_override.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::TableOverride;
        else if (s_current_transaction.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::CurrentTransaction;
    }
    else
        return false;

    auto explain_query = make_intrusive<ASTExplainQuery>(kind);

    {
        ASTPtr settings;
        ParserSetQuery parser_settings(true, false);

        auto begin = pos;
        if (parser_settings.parse(pos, settings, expected))
            explain_query->setSettings(std::move(settings));
        else
            pos = begin;
    }

    ParserCreateTableQuery create_p;
    ParserSelectWithUnionQuery select_p;
    ParserInsertQuery insert_p(end, allow_settings_after_format_in_insert);
    ParserSystemQuery system_p;
    ASTPtr query;
    if (kind == ASTExplainQuery::ExplainKind::ParsedAST)
    {
        ParserQuery p(end, allow_settings_after_format_in_insert);
        bool parsed_query = false;
        if (p.parse(pos, query, expected))
        {
            explain_query->setExplainedQuery(std::move(query));
            parsed_query = true;
        }
        /// Allow parentheses around inner EXPLAIN queries
        if (!parsed_query && pos->type == TokenType::OpeningRoundBracket)
        {
            auto saved = pos;
            ++pos;
            if (p.parse(pos, query, expected) && pos->type == TokenType::ClosingRoundBracket)
            {
                ++pos;
                explain_query->setExplainedQuery(std::move(query));
                parsed_query = true;
            }
            else
            {
                pos = saved;
            }
        }
        if (!parsed_query)
            return false;
    }
    else if (kind == ASTExplainQuery::ExplainKind::TableOverride)
    {
        ASTPtr table_function;
        if (!ParserFunction(true, true).parse(pos, table_function, expected))
            return false;
        ASTPtr table_override;
        if (!ParserTableOverrideDeclaration(false).parse(pos, table_override, expected))
            return false;
        explain_query->setTableFunction(table_function);
        explain_query->setTableOverride(table_override);
    }
    else if (kind == ASTExplainQuery::ExplainKind::QueryTree)
    {
        if (select_p.parse(pos, query, expected))
            explain_query->setExplainedQuery(std::move(query));
        else
            return false;
    }
    else if (kind == ASTExplainQuery::ExplainKind::CurrentTransaction)
    {
        /// Nothing to parse
    }
    else if (select_only)
    {
        if (select_p.parse(pos, query, expected))
            explain_query->setExplainedQuery(std::move(query));
        else
            return false;
    }
    else if (select_p.parse(pos, query, expected) ||
        create_p.parse(pos, query, expected) ||
        insert_p.parse(pos, query, expected) ||
        system_p.parse(pos, query, expected))
    {
        /// When the inner query is INSERT ... SELECT ... FORMAT <fmt>, the INSERT parser
        /// consumes the trailing FORMAT clause as part of itself. But for EXPLAIN, the
        /// FORMAT should apply to the EXPLAIN output, not to the inner INSERT.
        /// We only do this when there is no second FORMAT keyword following -- if there
        /// is one, the user wrote the double-FORMAT form explicitly and the first FORMAT
        /// genuinely belongs to the INSERT.
        /// We also keep the FORMAT on the INSERT when it describes the insert's input data,
        /// i.e. when the data is read FROM INFILE or via the `input` table function -- in
        /// those cases the format is required for the insert input, not the EXPLAIN output.
        if (auto * insert_query = query->as<ASTInsertQuery>())
        {
            ASTPtr input_function;
            insert_query->tryFindInputFunction(input_function);

            if (insert_query->select && !insert_query->format.empty() && insert_query->format != "Values"
                && !insert_query->settings_ast && !insert_query->infile && !input_function)
            {
                ParserKeyword s_format(Keyword::FORMAT);
                if (!s_format.checkWithoutMoving(pos, expected))
                {
                    /// Rewind past the format name identifier and the FORMAT keyword.
                    --pos;
                    --pos;
                    insert_query->format.clear();
                    insert_query->data = nullptr;
                    insert_query->end = nullptr;
                }
            }
        }

        explain_query->setExplainedQuery(std::move(query));
    }
    else
    {
        return false;
    }

    node = std::move(explain_query);
    return true;
}

}
