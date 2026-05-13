#include <Common/Exception.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/String.h>


namespace DB
{

bool ParserKQLStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Handle KQL let statements: let name = value;
    /// Store the binding and generate a no-op SELECT.
    /// KQL keywords are case-insensitive, so accept `LET`/`Let`/etc. as well.
    if (isValidKQLPos(pos) && pos->type == TokenType::BareWord
        && Poco::toLower(String(pos->begin, pos->end)) == "let")
    {
        auto let_pos = pos;
        ++pos;
        if (isValidKQLPos(pos) && pos->type == TokenType::BareWord)
        {
            String name(pos->begin, pos->end);
            ++pos;
            if (isValidKQLPos(pos) && String(pos->begin, pos->end) == "=")
            {
                ++pos;
                /// Collect the raw value tokens and also try to evaluate as KQL expression
                String raw_value;
                while (isValidKQLPos(pos) && pos->type != TokenType::Semicolon)
                {
                    if (!raw_value.empty()) raw_value += " ";
                    raw_value += String(pos->begin, pos->end);
                    ++pos;
                }

                /// Translate the value as a KQL expression. Any parse/semantic exception
                /// from `getExprFromToken` is propagated so that an invalid `let` value
                /// surfaces a clear syntax error instead of silently substituting raw
                /// tokens — consistent with all other `getExprFromToken` call sites.
                String value;
                {
                    Tokens val_tokens(raw_value.data(), raw_value.data() + raw_value.size(), 0, true);
                    IParser::Pos val_pos(val_tokens, pos.max_depth, pos.max_backtracks);
                    value = ParserKQLBase::getExprFromToken(val_pos);
                }
                if (value.empty())
                    value = raw_value;

                kqlLetBindings()[name] = value;
                /// Generate a no-op SELECT to consume the statement
                String noop_query = "SELECT 'ok' WHERE 0";
                Tokens noop_tokens(noop_query.data(), noop_query.data() + noop_query.size(), 0, true);
                IParser::Pos noop_pos(noop_tokens, pos.max_depth, pos.max_backtracks);
                ParserSelectWithUnionQuery select_p;
                return select_p.parse(noop_pos, node, expected);
            }
        }
        pos = let_pos;
    }

    ParserKQLWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);
    ParserSetQuery set_p;

    {
        if (set_p.parse(pos, node, expected))
        {
            /// Clear let bindings when the parsed `SET` actually changes `dialect`.
            /// Inspecting the AST avoids substring false positives (unrelated settings
            /// whose text happens to contain "kql"/"clickhouse") and case-sensitivity
            /// gaps (`SET dialect = 'KQL'`) that the raw text check used to have.
            if (const auto * set_ast = node ? node->as<ASTSetQuery>() : nullptr)
            {
                for (const auto & change : set_ast->changes)
                {
                    if (Poco::toLower(change.name) == "dialect")
                    {
                        kqlLetBindingsClear();
                        break;
                    }
                }
            }
            return true;
        }
    }

    bool res = query_with_output_p.parse(pos, node, expected);

    return res;
}

bool ParserKQLWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery kql_p;

    ASTPtr query;
    bool parsed = kql_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    node = std::move(query);
    return true;
}

bool ParserKQLWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // will support union next phase
    ASTPtr kql_query;

    if (!ParserKQLQuery().parse(pos, kql_query, expected))
        return false;

    if (kql_query->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(kql_query);
        return true;
    }

    auto list_node = make_intrusive<ASTExpressionList>();
    list_node->children.push_back(kql_query);

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    node = select_with_union_query;
    select_with_union_query->list_of_selects = list_node;
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    return true;
}

bool ParserKQLTableFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// TODO: This code is idiotic, see https://github.com/ClickHouse/ClickHouse/issues/61742

    ParserToken lparen(TokenType::OpeningRoundBracket);

    ASTPtr string_literal;
    ParserStringLiteral parser_string_literal;

    if (!lparen.ignore(pos, expected))
        return false;

    size_t paren_count = 0;
    String kql_statement;
    if (parser_string_literal.parse(pos, string_literal, expected))
    {
        kql_statement = typeid_cast<const ASTLiteral &>(*string_literal).value.safeGet<String>();
    }
    else
    {
        ++paren_count;
        auto pos_start = pos;
        while (isValidKQLPos(pos))
        {
            if (pos->type == TokenType::ClosingRoundBracket)
                --paren_count;
            if (pos->type == TokenType::OpeningRoundBracket)
                ++paren_count;

            if (paren_count == 0)
                break;
            ++pos;
        }
        if (!isValidKQLPos(pos))
        {
            return false;
        }
        --pos;
        kql_statement = String(pos_start->begin, pos->end);
        ++pos;
    }

    Tokens tokens_kql(kql_statement.data(), kql_statement.data() + kql_statement.size(), 0, true);
    IParser::Pos pos_kql(tokens_kql, pos.max_depth, pos.max_backtracks);

    Expected kql_expected;
    kql_expected.enable_highlighting = false;
    if (!ParserKQLWithUnionQuery().parse(pos_kql, node, kql_expected))
        return false;

    ++pos;
    return true;
}

}
