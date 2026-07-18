#include <Parsers/ParserCopyQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>

#include <algorithm>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

bool ParserCopyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_copy(Keyword::COPY);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_from(Keyword::FROM);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ParserSubquery s_subquery;

    auto copy_element = make_intrusive<ASTCopyQuery>();
    node = copy_element;

    if (!s_copy.ignore(pos, expected))
        return false;

    auto saved_pos = pos;

    if (!open_bracket.ignore(pos, expected))
    {
        ParserIdentifier s_table_identifier;
        ASTPtr table_name;
        if (!s_table_identifier.parse(pos, table_name, expected))
            return false;

        if (open_bracket.ignore(pos, expected))
        {
            ParserList columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
            ASTPtr columns;
            if (!columns_p.parse(pos, columns, expected))
                return false;
            if (!close_bracket.ignore(pos, expected))
                return false;

            for (const auto & column_ast : columns->children)
                copy_element->column_names.push_back(column_ast->as<ASTIdentifier>()->full_name);
        }
        saved_pos = pos;
        copy_element->table_name = table_name->as<ASTIdentifier>()->full_name;

        if (s_to.ignore(pos, expected))
        {
            copy_element->type = ASTCopyQuery::QueryType::COPY_TO;
        }
        else if (pos = saved_pos; s_from.ignore(pos, expected))
        {
            copy_element->type = ASTCopyQuery::QueryType::COPY_FROM;
        }
        else
        {
            return false;
        }

        if (pos->isEnd())
            return true;

        return parseOptions(pos, copy_element, expected);
    }

    pos = saved_pos;
    ASTPtr name_or_expr;
    if (!(s_ident.parse(pos, name_or_expr, expected) || ParserExpressionWithOptionalAlias(false).parse(pos, name_or_expr, expected)))
    {
        return false;
    }

    /// `COPY (query) TO STDOUT` - remember the inner query so that it can be executed as-is. Unwrap the
    /// subquery node so that the stored text is a runnable top-level query rather than `(SELECT ...)`.
    if (const auto * subquery_ast = name_or_expr->as<ASTSubquery>())
        copy_element->subquery = subquery_ast->children.at(0)->formatWithSecretsOneLine();
    else
        copy_element->subquery = name_or_expr->formatWithSecretsOneLine();

    saved_pos = pos;
    if (s_to.ignore(pos, expected))
    {
        copy_element->type = ASTCopyQuery::QueryType::COPY_TO;
    }
    else
    {
        return false;
    }

    if (pos->isEnd())
        return true;

    return parseOptions(pos, copy_element, expected);
}

namespace
{

/// Set the output format from a `COPY` option value. PostgreSQL keywords are case-insensitive, so match
/// against a normalized (lower-cased) spelling. `binary` is parsed but not supported: PostgreSQL binary
/// `COPY` has its own wire format and is rejected in the handler (see `PostgreSQLHandler::processCopyQuery`);
/// parsing it here yields a clear error there instead of the query silently falling through to the regular
/// query path.
void setCopyFormat(boost::intrusive_ptr<ASTCopyQuery> node, const String & raw_format)
{
    String format_name = raw_format;
    std::transform(format_name.begin(), format_name.end(), format_name.begin(), [](unsigned char c){ return std::tolower(c); });
    if (format_name == "csv")
        node->format = ASTCopyQuery::Formats::CSV;
    else if (format_name == "tsv" || format_name == "text")
        node->format = ASTCopyQuery::Formats::TSV;
    else if (format_name == "binary")
        node->format = ASTCopyQuery::Formats::Binary;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format in PostgreSQL COPY command: {}", raw_format);
}

/// A bare identifier that names an output format in the legacy `COPY ... CSV` / `WITH BINARY` grammar.
bool isCopyFormatKeyword(const String & lowercased_word)
{
    return lowercased_word == "csv" || lowercased_word == "tsv" || lowercased_word == "text" || lowercased_word == "binary";
}

}

bool ParserCopyQuery::parseOptions(Pos & pos, boost::intrusive_ptr<ASTCopyQuery> node, Expected & expected)
{
    ParserIdentifier s_output_identifier;
    ASTPtr output_name;
    if (!s_output_identifier.parse(pos, output_name, expected))
        return false;

    /// No options at all: PostgreSQL defaults to the text format, which we map to TSV. This is also the
    /// shape that libpq/pqxx use to read a result set (`COPY (query) TO STDOUT`).
    if (pos->isEnd())
        return true;

    /// The only `COPY` option we act on is the output format. It is spelled either as an explicit
    /// `FORMAT <name>` (PostgreSQL's `WITH (FORMAT csv)` and our own `WITH FORMAT csv`) or, in the legacy
    /// grammar, as a bare keyword (`CSV`, `TEXT`, `BINARY`), possibly surrounded by `WITH`, parentheses and
    /// other options. Every other option (`DELIMITER`, `NULL`, `HEADER`, ...) is accepted and ignored: real
    /// clients pass them - e.g. psycopg2's `copy_to`/`copy_from` always append `DELIMITER AS '\t' NULL AS
    /// '\N'`, which match our text defaults - and their values use client-specific escaping (`E'...'`
    /// strings, embedded control characters) that we deliberately do not try to interpret. Scanning for the
    /// format keyword and skipping everything else keeps those clients working while still selecting the
    /// requested format instead of silently falling back to TSV, and lets `binary` in any spelling reach the
    /// handler's rejection. Note that the option parser must not throw for these ignored options: an
    /// exception here makes `PostgreSQLHandler::processCopyQuery` fall through to the regular-query path,
    /// whose error tears the connection down mid-COPY (a driver such as psycopg2 then reports a lost
    /// connection instead of a clean error).
    ParserKeyword s_format(Keyword::FORMAT);
    ParserIdentifier s_identifier;
    ASTPtr name;
    while (!pos->isEnd())
    {
        if (s_format.ignore(pos, expected))
        {
            if (!s_identifier.parse(pos, name, expected))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a format name after FORMAT in the PostgreSQL COPY command");
            setCopyFormat(node, name->as<ASTIdentifier>()->full_name);
            continue;
        }

        if (s_identifier.parse(pos, name, expected))
        {
            String word = name->as<ASTIdentifier>()->full_name;
            std::transform(word.begin(), word.end(), word.begin(), [](unsigned char c){ return std::tolower(c); });
            if (isCopyFormatKeyword(word))
                setCopyFormat(node, word);
            /// Otherwise this identifier is an option name we do not interpret (`DELIMITER`, `NULL`, ...);
            /// its value, if any, is skipped by the token-at-a-time advance below.
            continue;
        }

        /// A token that is neither `FORMAT` nor an identifier: `WITH`, a parenthesis, a comma, or an option
        /// value such as a string literal. Skip it.
        ++pos;
    }

    return true;
}

}
