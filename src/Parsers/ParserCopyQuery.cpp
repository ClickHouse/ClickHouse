#include <Parsers/ParserCopyQuery.h>

#include <Common/quoteString.h>
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
        ParserCompoundIdentifier s_table_identifier;
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

            /// Preserve each column as one quoted SQL identifier.
            for (const auto & column_ast : columns->children)
                copy_element->column_names.push_back(backQuoteIfNeed(column_ast->as<ASTIdentifier>()->full_name));
        }
        saved_pos = pos;
        /// Quote each part of a compound table name separately.
        {
            const auto & id = table_name->as<ASTIdentifier &>();
            String rendered;
            for (const auto & part : id.name_parts)
            {
                if (!rendered.empty())
                    rendered += '.';
                rendered += backQuoteIfNeed(part);
            }
            copy_element->table_name = rendered;
        }

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

String toLowerCase(std::string_view name)
{
    String result(name);
    std::transform(result.begin(), result.end(), result.begin(), [](char c) { return static_cast<char>(std::tolower(c)); });
    return result;
}

void setFormat(const String & format_name, boost::intrusive_ptr<ASTCopyQuery> node)
{
    /// `text` is what PostgreSQL calls its default format, and it is tab separated; `tsv` is accepted
    /// under its ClickHouse name.
    if (format_name == "text" || format_name == "tsv")
        node->format = ASTCopyQuery::Formats::TSV;
    else if (format_name == "csv")
        node->format = ASTCopyQuery::Formats::CSV;
    else if (format_name == "binary")
        node->format = ASTCopyQuery::Formats::Binary;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format from postgresql copy command {}", format_name);
}

/// A bare word, a quoted identifier or a number - whatever it is, the caller decides what to make of it.
bool parseWord(IParser::Pos & pos, String & word)
{
    if (pos->type != TokenType::BareWord && pos->type != TokenType::Number && pos->type != TokenType::QuotedIdentifier)
        return false;

    word = String(pos->begin, pos->end);
    if (pos->type == TokenType::QuotedIdentifier)
        word = word.substr(1, word.size() - 2);

    ++pos;
    return true;
}

/// The value of an option, with PostgreSQL's optional noise word `AS` in front of it.
bool parseOptionValue(IParser::Pos & pos, Expected & expected, String & value)
{
    ParserKeyword s_as(Keyword::AS);
    s_as.ignore(pos, expected);

    ASTPtr literal;
    if (ParserStringLiteral().parse(pos, literal, expected))
    {
        value = literal->as<ASTLiteral &>().value.safeGet<String>();
        return true;
    }

    return parseWord(pos, value);
}

/// The options whose value decides how the data is written. They are accepted when the client asks
/// for what ClickHouse writes anyway - `psycopg2` spells the defaults out on every `copy_to` and
/// `copy_from` - and refused otherwise: writing a different shape than the client asked for is
/// exactly what this option list used to do silently.
struct DataShapeOptions
{
    std::optional<String> delimiter;
    std::optional<String> null_value;
    std::optional<String> quote;
};

void checkDataShapeOptions(const DataShapeOptions & options, const ASTCopyQuery & node)
{
    const bool is_csv = node.format == ASTCopyQuery::Formats::CSV;

    if (options.delimiter && *options.delimiter != (is_csv ? "," : "\t"))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Option DELIMITER of the postgresql copy command is only supported with the default delimiter of the {} format",
            toString(node.format));

    /// The representation of NULL in both the TSV and the CSV format of ClickHouse.
    if (options.null_value && *options.null_value != "\\N")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Option NULL of the postgresql copy command is only supported with the value '\\N'");

    if (options.quote)
    {
        if (!is_csv)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Option QUOTE of the postgresql copy command applies to the csv format only");
        if (*options.quote != "\"")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Option QUOTE of the postgresql copy command is only supported with the value '\"'");
    }
}

bool parseOption(IParser::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTCopyQuery> node, DataShapeOptions & data_shape_options)
{
    const String option_as_written(pos->begin, pos->end);

    String option;
    if (!parseWord(pos, option))
        return false;
    option = toLowerCase(option);

    if (option == "format")
    {
        String format_name;
        if (!parseOptionValue(pos, expected, format_name))
            return false;
        setFormat(toLowerCase(format_name), node);
    }
    else if (option == "csv" || option == "binary" || option == "text")
    {
        /// The legacy spelling of the format: WITH [BINARY] [CSV].
        setFormat(option, node);
    }
    else if (option == "header")
    {
        /// `true`/`false`/`on`/`off`/`1`/`0`, or nothing at all, which PostgreSQL reads as `true`.
        String value;
        auto value_pos = pos;
        if (!parseOptionValue(pos, expected, value))
        {
            node->header = true;
            return true;
        }

        const String lower_value = toLowerCase(value);
        if (lower_value == "true" || lower_value == "on" || lower_value == "1")
            node->header = true;
        else if (lower_value == "false" || lower_value == "off" || lower_value == "0")
            node->header = false;
        else
        {
            pos = value_pos;
            node->header = true;
        }
    }
    else if (option == "delimiter" || option == "null" || option == "quote")
    {
        String value;
        if (!parseOptionValue(pos, expected, value))
            return false;

        if (option == "delimiter")
            data_shape_options.delimiter = value;
        else if (option == "null")
            data_shape_options.null_value = value;
        else
            data_shape_options.quote = value;
    }
    else
    {
        /// ENCODING, ESCAPE, FORCE_QUOTE and the rest change the data as well, and there is no
        /// version of them this protocol can serve.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Option {} of the postgresql copy command is not supported", option_as_written);
    }

    return true;
}

}

bool ParserCopyQuery::parseOptions(Pos & pos, boost::intrusive_ptr<ASTCopyQuery> node, Expected & expected)
{
    ParserIdentifier s_output_identifier;
    ASTPtr output_name;
    if (!s_output_identifier.parse(pos, output_name, expected))
        return false;

    ParserKeyword s_with(Keyword::WITH);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);

    auto assert_end = [&]
    {
        /// Transferring the data in the default format because the rest of the command was not
        /// understood would hand the client rows it cannot parse, or store rows parsed the wrong way,
        /// so say that it was not understood instead.
        if (!pos->isEnd())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Unknown part of the postgresql copy command: {}", String(pos->begin, pos->end));
    };

    if (!s_with.ignore(pos, expected))
    {
        assert_end();
        return true;
    }

    DataShapeOptions data_shape_options;

    /// The form every modern client sends: WITH (FORMAT csv, HEADER true, ...).
    if (open_bracket.ignore(pos, expected))
    {
        bool is_first_option = true;
        while (!close_bracket.ignore(pos, expected))
        {
            if (!is_first_option && !comma.ignore(pos, expected))
                return false;
            is_first_option = false;

            if (!parseOption(pos, expected, node, data_shape_options))
                return false;
        }
    }
    else
    {
        /// The legacy spelling, which `psql` and the client libraries still use:
        /// WITH [BINARY] [CSV [HEADER]] [DELIMITER [AS] 'c'] [NULL [AS] 's'] [QUOTE [AS] 'c'].
        /// `WITH FORMAT csv` is not PostgreSQL syntax at all, but this protocol has accepted it from
        /// the beginning, so it is parsed here too.
        while (!pos->isEnd())
        {
            if (!parseOption(pos, expected, node, data_shape_options))
                return false;
        }
    }

    checkDataShapeOptions(data_shape_options, *node);
    assert_end();

    return true;
}

}
