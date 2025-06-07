#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace
{
bool parseUserNameWithHost(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTUserNameWithHost> & ast_)
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            ASTPtr name_ast;
            String host_pattern;

            if (ParserIdentifier(/*allow_query_parameter_=*/true).parse(pos, name_ast, expected))
            {
                if (ParserToken{TokenType::At}.ignore(pos, expected))
                    if (!parseIdentifierOrStringLiteral(pos, expected, host_pattern) || host_pattern.empty())
                        return false;
            }
            else if (ParserStringLiteral{}.parse(pos, name_ast, expected))
            {
                if (name_ast->as<ASTLiteral &>().value.safeGet<String>().empty())
                    return false;
            }
            else
            {
                return false;
            }

            boost::algorithm::trim(host_pattern);

            ast_ = std::make_shared<ASTUserNameWithHost>(std::move(name_ast), std::move(host_pattern));
            return true;
        });
}
}


bool ParserUserNameWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::shared_ptr<ASTUserNameWithHost> res;
    if (!parseUserNameWithHost(pos, expected, res))
        return false;

    node = res;
    return true;
}


bool ParserUserNamesWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTs names;

    auto parse_single_name = [&]
    {
        std::shared_ptr<ASTUserNameWithHost> ast;
        if (!parseUserNameWithHost(pos, expected, ast))
            return false;

        names.emplace_back(std::move(ast));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_single_name, false))
        return false;

    auto result = std::make_shared<ASTUserNamesWithHost>();
    result->children = std::move(names);
    node = result;
    return true;
}

}
