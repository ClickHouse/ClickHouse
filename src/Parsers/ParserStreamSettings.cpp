#include <Core/Field.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTStreamSettings.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserStreamSettings.h>

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

}

bool ParserStreamSettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_cursor{Keyword::CURSOR};

    ASTStreamSettings::StreamSettings settings;

    if (s_cursor.ignore(pos, expected))
    {
        Map flat;
        if (!parseCursorObject(pos, expected, flat, ""))
            return false;

        settings.cursor_tree = std::move(flat);
    }

    node = make_intrusive<ASTStreamSettings>(std::move(settings));

    return true;
}

}
