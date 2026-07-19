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
#include <optional>

#include <fmt/format.h>

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

String toLowerCopy(const String & word)
{
    String result = word;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c){ return std::tolower(c); });
    return result;
}

/// The bytes of a string-literal token with the surrounding quotes removed and no further unescaping.
/// PostgreSQL clients render option values (such as a `DELIMITER`) in their own language before sending them
/// - psycopg2, for instance, passes the actual tab byte for its default text delimiter - so the bytes
/// between the quotes are exactly the value the client means. This deliberately does not apply ClickHouse's
/// backslash unescaping, which would misread e.g. the text null marker `\N`.
String stringLiteralInnerBytes(const String & raw_token)
{
    if (raw_token.size() >= 2 && (raw_token.front() == '\'' || raw_token.front() == '"'))
        return raw_token.substr(1, raw_token.size() - 2);
    return raw_token;
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

    /// The `COPY` option we act on directly is the output format: an explicit `FORMAT <name>` (PostgreSQL's
    /// `WITH (FORMAT csv)` and our own `WITH FORMAT csv`) or, in the legacy grammar, a bare keyword (`CSV`,
    /// `TEXT`, `BINARY`), possibly surrounded by `WITH`, parentheses and commas. `binary` in any spelling is
    /// carried through so the handler rejects it with a clean message.
    ///
    /// The data-formatting options are handled as follows so that a client's request is never silently
    /// disregarded (which would emit output that does not match what it asked for). A `DELIMITER` and a
    /// `HEADER` that match our defaults for the chosen format (a tab for text/TSV, a comma for CSV, and no
    /// header) are no-ops and accepted - this is exactly what real clients append, e.g. psycopg2's
    /// `copy_to`/`copy_from` always send `DELIMITER AS '\t' NULL AS '\N'`. A non-default `DELIMITER`, a
    /// `HEADER`, or any option we do not interpret (`QUOTE`, `ESCAPE`, `ENCODING`, ...) is recorded in
    /// `unsupported_option`; the handler then rejects the command with an `ErrorResponse`. `NULL` is accepted
    /// and ignored: its value only matters for actual NULLs, its default representation differs between
    /// PostgreSQL and ClickHouse, and interpreting it faithfully is left to a dedicated follow-up.
    ///
    /// The parser must not throw for these options: an exception here makes
    /// `PostgreSQLHandler::processCopyQuery` fall through to the regular-query path, whose error tears the
    /// connection down mid-COPY (a driver such as psycopg2 then reports a lost connection instead of a clean
    /// error), which is why the rejection is deferred to the handler.
    enum class PendingOption : uint8_t { None, Delimiter, Null, Header };
    PendingOption pending = PendingOption::None;
    std::optional<String> delimiter_value;
    bool header_requested = false;
    String unknown_option;

    while (!pos->isEnd())
    {
        if (pos->type == TokenType::BareWord)
        {
            String word(pos->begin, pos->end);
            String lower = toLowerCopy(word);
            if (lower == "format")
            {
                ++pos;
                if (pos->isEnd() || pos->type != TokenType::BareWord)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a format name after FORMAT in the PostgreSQL COPY command");
                setCopyFormat(node, String(pos->begin, pos->end));
                pending = PendingOption::None;
            }
            else if (isCopyFormatKeyword(lower))
            {
                setCopyFormat(node, lower);
                pending = PendingOption::None;
            }
            else if (lower == "with" || lower == "as")
            {
                /// Filler keywords: keep whatever option is pending so `DELIMITER AS '...'` still binds.
            }
            else if (lower == "delimiter")
                pending = PendingOption::Delimiter;
            else if (lower == "null")
                pending = PendingOption::Null;
            else if (lower == "header")
            {
                header_requested = true;
                pending = PendingOption::Header;
            }
            else if (lower == "true" || lower == "on")
            {
                if (pending == PendingOption::Header)
                    header_requested = true;
                pending = PendingOption::None;
            }
            else if (lower == "false" || lower == "off")
            {
                if (pending == PendingOption::Header)
                    header_requested = false;
                pending = PendingOption::None;
            }
            else
            {
                if (unknown_option.empty())
                    unknown_option = word;
                pending = PendingOption::None;
            }
            ++pos;
        }
        else if (pos->type == TokenType::StringLiteral)
        {
            if (pending == PendingOption::Delimiter)
                delimiter_value = stringLiteralInnerBytes(String(pos->begin, pos->end));
            pending = PendingOption::None;
            ++pos;
        }
        else
        {
            /// Parentheses, commas and other punctuation carry no option value.
            ++pos;
        }
    }

    if (!unknown_option.empty())
        node->unsupported_option = fmt::format("the \"{}\" option", unknown_option);
    else
    {
        const String default_delimiter = node->format == ASTCopyQuery::Formats::CSV ? "," : "\t";
        if (delimiter_value && *delimiter_value != default_delimiter)
            node->unsupported_option = "a non-default DELIMITER";
        else if (header_requested)
            node->unsupported_option = "HEADER";
    }

    return true;
}

}
