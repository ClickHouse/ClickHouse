#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
bool parseUserNameWithHost(
    IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTUserNameWithHost> & ast, bool allow_query_parameter)
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            ASTPtr name_ast;
            String host_pattern;

            if (ParserIdentifier(allow_query_parameter).parse(pos, name_ast, expected))
            {
                if (ParserToken{TokenType::At}.ignore(pos, expected))
                    if (!parseIdentifierOrStringLiteral(pos, expected, host_pattern) || host_pattern.empty())
                        return false;
            }
            else if (ParserStringLiteral{}.parse(pos, name_ast, expected))
            {
                auto name = name_ast->as<ASTLiteral &>().value.safeGet<String>();

                if (name.empty())
                    return false;

                // If somebody tried to use query parameter by putting it into a string literal my mistake, e.g.
                //     CREATE USER '{name:Identifier}@192.168.%.%', '{name:Identifier}';
                // they would end up with not intended users in their system because string literals are parsed as is.
                // For preventing this kind of mistakes we parse string literals and allow only identifiers without parameters.
                Tokens tokens(name.data(), name.data() + name.size(), 0, false);
                IParser::Pos literal_check_pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

                Expected literal_check_expected;
                ASTPtr literal_check_ast;
                if (ParserIdentifier(allow_query_parameter = true).parse(literal_check_pos, literal_check_ast, literal_check_expected)
                    && literal_check_ast->as<ASTIdentifier &>().isParam())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "String literals don't support query parameters substitution, remove single quotes surrounding the query "
                        "parameter");
            }
            else
            {
                return false;
            }

            boost::algorithm::trim(host_pattern);

            if (host_pattern.empty() || host_pattern == "%")
                ast = std::make_shared<ASTUserNameWithHost>(std::move(name_ast));
            else
                ast = std::make_shared<ASTUserNameWithHost>(std::move(name_ast), std::move(host_pattern));

            return true;
        });
}
}


bool ParserUserNameWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::shared_ptr<ASTUserNameWithHost> res;
    if (!parseUserNameWithHost(pos, expected, res, allow_query_parameter))
        return false;

    node = res;
    return true;
}

ParserUserNameWithHost::ParserUserNameWithHost(bool allow_query_parameter_)
    : allow_query_parameter(allow_query_parameter_)
{
}


bool ParserUserNamesWithHost::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTs names;

    auto parse_single_name = [&]
    {
        std::shared_ptr<ASTUserNameWithHost> ast;
        if (!parseUserNameWithHost(pos, expected, ast, allow_query_parameter))
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

ParserUserNamesWithHost::ParserUserNamesWithHost(bool allow_query_parameter_)
    : allow_query_parameter(allow_query_parameter_)
{
}

}
