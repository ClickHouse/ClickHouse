#include <Parsers/ParserDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserCreateQuery.h>

namespace DB
{

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNestedTable nested;
    if (nested.parse(pos, node, expected))
        return true;

    String type_name;

    ParserIdentifier name_parser;
    ASTPtr identifier;
    if (!name_parser.parse(pos, identifier, expected))
        return false;
    tryGetIdentifierNameInto(identifier, type_name);

    String type_name_upper = Poco::toUpper(type_name);
    String type_name_suffix;

    /// Special cases for compatibility with SQL standard. We can parse several words as type name
    /// only for certain first words, otherwise we don't know how many words to parse
    if (type_name_upper == "NATIONAL")
    {
        if (ParserKeyword("CHARACTER LARGE OBJECT").ignore(pos))
            type_name_suffix = "CHARACTER LARGE OBJECT";
        else if (ParserKeyword("CHARACTER VARYING").ignore(pos))
            type_name_suffix = "CHARACTER VARYING";
        else if (ParserKeyword("CHAR VARYING").ignore(pos))
            type_name_suffix = "CHAR VARYING";
        else if (ParserKeyword("CHARACTER").ignore(pos))
            type_name_suffix = "CHARACTER";
        else if (ParserKeyword("CHAR").ignore(pos))
            type_name_suffix = "CHAR";
    }
    else if (type_name_upper == "BINARY" ||
             type_name_upper == "CHARACTER" ||
             type_name_upper == "CHAR" ||
             type_name_upper == "NCHAR")
    {
        if (ParserKeyword("LARGE OBJECT").ignore(pos))
            type_name_suffix = "LARGE OBJECT";
        else if (ParserKeyword("VARYING").ignore(pos))
            type_name_suffix = "VARYING";
    }
    else if (type_name_upper == "DOUBLE")
    {
        if (ParserKeyword("PRECISION").ignore(pos))
            type_name_suffix = "PRECISION";
    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = type_name;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = function_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ParserList args_parser(std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma));
    ASTPtr expr_list_args;

    if (!args_parser.parse(pos, expr_list_args, expected))
        return false;
    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = function_node;
    return true;
}

}

