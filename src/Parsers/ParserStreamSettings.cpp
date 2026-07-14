#include <Core/Field.h>

#include <Common/IntervalKind.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTStreamSettings.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserStreamSettings.h>
#include <Parsers/parseIntervalKind.h>

namespace DB
{

namespace
{

/// Recursively parses a nested JSON-like object
///   {'key': int | {'key': int | {...}}, ...}
/// and flattens it into `flat` with dotted-path keys.
bool parseCursorObject(IParser::Pos & pos, Expected & expected, Map & flat, const String & path_prefix)
{
    ParserToken l_br(TokenType::OpeningCurlyBrace);
    ParserToken r_br(TokenType::ClosingCurlyBrace);
    ParserToken comma(TokenType::Comma);
    ParserToken colon(TokenType::Colon);
    ParserStringLiteral key_p;
    ParserUnsignedInteger value_p;

    if (!l_br.ignore(pos, expected))
        return false;

    bool first = true;
    while (!r_br.ignore(pos, expected))
    {
        if (!first && !comma.ignore(pos, expected))
            return false;
        first = false;

        ASTPtr key_ast;
        if (!key_p.parse(pos, key_ast, expected))
            return false;

        const String & key = key_ast->as<ASTLiteral>()->value.safeGet<String>();

        if (!colon.ignore(pos, expected))
            return false;

        const String new_path = path_prefix.empty() ? key : (path_prefix + "." + key);

        /// Peek at next token: `{` starts a nested object, integer is a leaf.
        if (pos->type == TokenType::OpeningCurlyBrace)
        {
            if (!parseCursorObject(pos, expected, flat, new_path))
                return false;
        }
        else
        {
            ASTPtr val_ast;
            if (!value_p.parse(pos, val_ast, expected))
                return false;

            flat.push_back(Tuple{new_path, val_ast->as<ASTLiteral>()->value});
        }
    }

    return true;
}

std::optional<Map> parseCursorClause(IParser::Pos & pos, Expected & expected)
{
    Map flat;
    if (!parseCursorObject(pos, expected, flat, ""))
        return std::nullopt;
    return flat;
}

std::optional<ASTStreamSettings::WatermarkSettings> parseWatermarkClause(IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_for{Keyword::FOR};
    if (!s_for.ignore(pos, expected))
        return std::nullopt;

    ASTPtr column_ast;
    ParserIdentifier identifier_p;
    if (!identifier_p.parse(pos, column_ast, expected))
        return std::nullopt;

    ParserKeyword s_as{Keyword::AS};
    if (!s_as.ignore(pos, expected))
        return std::nullopt;

    ASTPtr expression_ast;
    ParserExpression expression_p;
    if (!expression_p.parse(pos, expression_ast, expected))
        return std::nullopt;

    ASTStreamSettings::WatermarkSettings watermark;
    watermark.column = getIdentifierName(column_ast);
    watermark.expression = std::move(expression_ast);

    ParserKeyword s_idle_timeout{Keyword::IDLE_TIMEOUT};
    if (s_idle_timeout.ignore(pos, expected))
    {
        if (!ParserKeyword{Keyword::INTERVAL}.ignore(pos, expected))
            return std::nullopt;

        ASTPtr num_intervals_ast;
        if (!ParserUnsignedInteger{}.parse(pos, num_intervals_ast, expected))
            return std::nullopt;

        IntervalKind interval_kind;
        if (!parseIntervalKind(pos, expected, interval_kind))
            return std::nullopt;

        const auto num_intervals = num_intervals_ast->as<ASTLiteral &>().value.safeGet<UInt64>();
        watermark.idle_timeout_ms = static_cast<Int64>(num_intervals) * interval_kind.toAvgMilliseconds();
    }

    return watermark;
}

}

bool ParserStreamSettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_cursor{Keyword::CURSOR};
    ParserKeyword s_watermark{Keyword::WATERMARK};

    auto stream_settings = make_intrusive<ASTStreamSettings>();

    if (s_cursor.ignore(pos, expected))
    {
        auto cursor = parseCursorClause(pos, expected);
        if (!cursor.has_value())
            return false;

        stream_settings->cursor = std::move(cursor);
    }

    if (s_watermark.ignore(pos, expected))
    {
        auto watermark = parseWatermarkClause(pos, expected);
        if (!watermark.has_value())
            return false;

        stream_settings->watermark = std::move(watermark);
    }

    node = std::move(stream_settings);

    return true;
}

}
