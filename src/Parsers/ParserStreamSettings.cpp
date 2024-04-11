#include <optional>
#include <Core/Field.h>
#include <Core/Streaming/ReadingStage.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTStreamSettings.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserStreamSettings.h>

namespace DB
{

namespace
{

class ParserCursor : public IParserBase
{
public:
protected:
    const char * getName() const override { return "Cursor Map"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserToken l_br(TokenType::OpeningCurlyBrace);
        ParserToken r_br(TokenType::ClosingCurlyBrace);
        ParserToken comma(TokenType::Comma);
        ParserToken colon(TokenType::Colon);
        ParserStringLiteral key_p;
        ParserUnsignedInteger value_p;

        if (!l_br.ignore(pos, expected))
            return false;

        Map map;

        while (!r_br.ignore(pos, expected))
        {
            if (!map.empty() && !comma.ignore(pos, expected))
                return false;

            ASTPtr key;
            ASTPtr val;

            if (!key_p.parse(pos, key, expected))
                return false;

            if (!colon.ignore(pos, expected))
                return false;

            if (!value_p.parse(pos, val, expected))
                return false;

            ASTLiteral * key_literal = key->as<ASTLiteral>();
            ASTLiteral * val_literal = val->as<ASTLiteral>();

            if (key_literal->value.getType() != Field::Types::String)
                return false;

            map.push_back(Tuple{std::move(key_literal->value), std::move(val_literal->value)});
        }

        node = std::make_shared<ASTLiteral>(std::move(map));

        return true;
    }
};

}

bool ParserStreamSettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_tail{Keyword::TAIL};
    ParserKeyword s_cursor{Keyword::CURSOR};

    StreamReadingStage stage = StreamReadingStage::AllData;
    std::optional<String> keeper_key;
    std::optional<Map> collapsed_tree;

    if (s_tail.ignore(pos, expected))
        stage = StreamReadingStage::TailOnly;
    else if (s_cursor.ignore(pos, expected))
    {
        ParserStringLiteral keeper_key_p;
        ParserCursor cursor_p;

        ASTPtr keeper_key_ast;
        ASTPtr collapsed_tree_ast;

        if (keeper_key_p.parse(pos, keeper_key_ast, expected))
            keeper_key = keeper_key_ast->as<ASTLiteral>()->value.safeGet<String>();

        if (cursor_p.parse(pos, collapsed_tree_ast, expected))
            collapsed_tree = collapsed_tree_ast->as<ASTLiteral>()->value.safeGet<Map>();

        if (keeper_key_ast == nullptr && collapsed_tree_ast == nullptr)
            return false;
    }

    node = std::make_shared<ASTStreamSettings>(stage, std::move(keeper_key), std::move(collapsed_tree));

    return true;
}

}
