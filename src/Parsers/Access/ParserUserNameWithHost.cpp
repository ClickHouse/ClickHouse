#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string.hpp>


namespace DB
{
namespace
{
    bool parseUserNameWithHost(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTUserNameWithHost> & ast)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String base_name;
            if (!parseIdentifierOrStringLiteral(pos, expected, base_name))
                return false;

            boost::algorithm::trim(base_name);

            String host_pattern;
            if (ParserToken{TokenType::At}.ignore(pos, expected))
            {
                if (!parseIdentifierOrStringLiteral(pos, expected, host_pattern))
                    return false;

                boost::algorithm::trim(host_pattern);
                if (host_pattern == "%")
                    host_pattern.clear();
            }

            ast = std::make_shared<ASTUserNameWithHost>();
            ast->base_name = std::move(base_name);
            ast->host_pattern = std::move(host_pattern);
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
    std::vector<std::shared_ptr<ASTUserNameWithHost>> names;

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
    result->names = std::move(names);
    node = result;
    return true;
}

}
