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

bool ParserCopyQuery::parseOptions(Pos & pos, boost::intrusive_ptr<ASTCopyQuery> node, Expected & expected)
{
    ParserIdentifier s_output_identifier;
    ASTPtr output_name;
    if (!s_output_identifier.parse(pos, output_name, expected))
        return false;

    ParserKeyword s_with(Keyword::WITH);
    ParserKeyword s_format(Keyword::FORMAT);

    s_with.ignore(pos, expected);

    if (s_format.ignore(pos, expected))
    {
        ParserIdentifier s_format_identifier;
        ASTPtr format;
        if (!s_format_identifier.parse(pos, format, expected))
            return false;

        /// PostgreSQL keywords are case-insensitive, so match against a normalized (lower-cased) spelling.
        String format_name = format->as<ASTIdentifier>()->full_name;
        std::transform(format_name.begin(), format_name.end(), format_name.begin(), [](unsigned char c){ return std::tolower(c); });
        if (format_name == "csv")
            node->format = ASTCopyQuery::Formats::CSV;
        else if (format_name == "tsv" || format_name == "text")
            node->format = ASTCopyQuery::Formats::TSV;
        else if (format_name == "binary")
            /// Parsed but not supported: PostgreSQL binary `COPY` has its own wire format, rejected in the
            /// handler (see `PostgreSQLHandler::processCopyQuery`). Parsing it here yields a clear error
            /// there instead of the query silently falling through to the regular query path.
            node->format = ASTCopyQuery::Formats::Binary;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format in PostgreSQL COPY command: {}", format->as<ASTIdentifier>()->full_name);
    }

    while (!pos->isEnd())
        ++pos;

    return true;
}

}
