#include <Parsers/Access/ParserPublicSSHKey.h>

#include <Parsers/Access/ASTPublicSSHKey.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{

namespace
{
    bool parsePublicSSHKey(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTPublicSSHKey> & ast)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String key_base64;
            if (!ParserKeyword{Keyword::KEY}.ignore(pos, expected) || !parseIdentifierOrStringLiteral(pos, expected, key_base64))
                return false;

            String type;
            if (!ParserKeyword{Keyword::TYPE}.ignore(pos, expected) || !parseIdentifierOrStringLiteral(pos, expected, type))
                return false;

            ast = std::make_shared<ASTPublicSSHKey>();
            ast->key_base64 = std::move(key_base64);
            ast->type = std::move(type);
            return true;
        });
    }
}


bool ParserPublicSSHKey::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::shared_ptr<ASTPublicSSHKey> res;
    if (!parsePublicSSHKey(pos, expected, res))
        return false;

    node = res;
    return true;
}

}
