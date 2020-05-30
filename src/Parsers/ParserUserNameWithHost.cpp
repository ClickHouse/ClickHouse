#include <Parsers/ParserUserNameWithHost.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string.hpp>


namespace DB
{
bool ParserUserNameWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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

    auto result = std::make_shared<ASTUserNameWithHost>();
    result->base_name = std::move(base_name);
    result->host_pattern = std::move(host_pattern);
    node = result;
    return true;
}


bool ParserUserNamesWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<std::shared_ptr<ASTUserNameWithHost>> names;
    do
    {
        ASTPtr ast;
        if (!ParserUserNameWithHost{}.parse(pos, ast, expected))
            return false;

        names.emplace_back(typeid_cast<std::shared_ptr<ASTUserNameWithHost>>(ast));
    }
    while (ParserToken{TokenType::Comma}.ignore(pos, expected));

    auto result = std::make_shared<ASTUserNamesWithHost>();
    result->names = std::move(names);
    node = result;
    return true;
}

}
