#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    bool parseUserNameWithHost(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTUserNameWithHost> & ast)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String base_name;
            if (!parseIdentifierOrStringLiteral(pos, expected, base_name))
                return false;

            /// Previously username was silently trimmed. Now we throw an exception instead.
            /// But it's not clear why spaces were not allowed.
            if (base_name.empty() || base_name.starts_with(' ') || base_name.ends_with(' '))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name cannot start or end with spaces and cannot be empty");

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
