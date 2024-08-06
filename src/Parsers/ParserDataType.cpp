#include <Parsers/ParserDataType.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace
{

/// Parser of Dynamic type arguments: Dynamic(max_types=N)
class DynamicArgumentsParser : public IParserBase
{
private:
    const char * getName() const override { return "Dynamic data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ASTPtr identifier;
        ParserIdentifier identifier_parser;
        if (!identifier_parser.parse(pos, identifier, expected))
            return false;

        if (pos->type != TokenType::Equals)
        {
            expected.add(pos, "equals operator");
            return false;
        }

        ++pos;

        ASTPtr number;
        ParserNumber number_parser;
        if (!number_parser.parse(pos, number, expected))
            return false;

        node = makeASTFunction("equals", identifier, number);
        return true;
    }
};

/// Wrapper to allow mixed lists of nested and normal types.
/// Parameters are either:
/// - Nested table elements;
/// - Enum element in form of 'a' = 1;
/// - literal;
/// - Dynamic type arguments;
/// - another data type (or identifier);
class ParserDataTypeArgument : public IParserBase
{
public:
    explicit ParserDataTypeArgument(std::string_view type_name_) : type_name(type_name_)
    {
    }

private:
    const char * getName() const override { return "data type argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        if (type_name == "Dynamic")
        {
            DynamicArgumentsParser parser;
            return parser.parse(pos, node, expected);
        }

        ParserNestedTable nested_parser;
        ParserDataType data_type_parser;
        ParserAllCollectionsOfLiterals literal_parser(false);

        const char * operators[] = {"=", "equals", nullptr};
        ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>());

        if (pos->type == TokenType::BareWord && std::string_view(pos->begin, pos->size()) == "Nested")
            return nested_parser.parse(pos, node, expected);

        return enum_parser.parse(pos, node, expected)
            || literal_parser.parse(pos, node, expected)
            || data_type_parser.parse(pos, node, expected);
    }

    std::string_view type_name;
};

}

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

    /// When parsing we accept quoted type names (e.g. `UInt64`), but when formatting we print them
    /// unquoted (e.g. UInt64). This introduces problems when the string in the quotes is garbage:
    ///  * Array(`x.y`) -> Array(x.y) -> fails to parse
    ///  * `Null` -> Null -> parses as keyword instead of type name
    /// Here we check for these cases and reject.
    if (!std::all_of(type_name.begin(), type_name.end(), [](char c) { return isWordCharASCII(c) || c == '$'; }))
    {
        expected.add(pos, "type name");
        return false;
    }
    /// Keywords that IParserColumnDeclaration recognizes before the type name.
    /// E.g. reject CREATE TABLE a (x `Null`) because in "x Null" the Null would be parsed as
    /// column attribute rather than type name.
    {
        String n = type_name;
        boost::to_upper(n);
        if (n == "NOT" || n == "NULL" || n == "DEFAULT" || n == "MATERIALIZED" || n == "EPHEMERAL" || n == "ALIAS" || n == "AUTO" || n == "PRIMARY" || n == "COMMENT" || n == "CODEC")
        {
            expected.add(pos, "type name");
            return false;
        }
    }

    String type_name_upper = Poco::toUpper(type_name);
    String type_name_suffix;

    /// Special cases for compatibility with SQL standard. We can parse several words as type name
    /// only for certain first words, otherwise we don't know how many words to parse
    if (type_name_upper == "NATIONAL")
    {
        if (ParserKeyword(Keyword::CHARACTER_LARGE_OBJECT).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER_LARGE_OBJECT);
        else if (ParserKeyword(Keyword::CHARACTER_VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER_VARYING);
        else if (ParserKeyword(Keyword::CHAR_VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHAR_VARYING);
        else if (ParserKeyword(Keyword::CHARACTER).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER);
        else if (ParserKeyword(Keyword::CHAR).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHAR);
    }
    else if (type_name_upper == "BINARY" ||
             type_name_upper == "CHARACTER" ||
             type_name_upper == "CHAR" ||
             type_name_upper == "NCHAR")
    {
        if (ParserKeyword(Keyword::LARGE_OBJECT).ignore(pos))
            type_name_suffix = toStringView(Keyword::LARGE_OBJECT);
        else if (ParserKeyword(Keyword::VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::VARYING);
    }
    else if (type_name_upper == "DOUBLE")
    {
        if (ParserKeyword(Keyword::PRECISION).ignore(pos))
            type_name_suffix = toStringView(Keyword::PRECISION);
    }
    else if (type_name_upper.find("INT") != std::string::npos)
    {
        /// Support SIGNED and UNSIGNED integer type modifiers for compatibility with MySQL
        if (ParserKeyword(Keyword::SIGNED).ignore(pos, expected))
            type_name_suffix = toStringView(Keyword::SIGNED);
        else if (ParserKeyword(Keyword::UNSIGNED).ignore(pos, expected))
            type_name_suffix = toStringView(Keyword::UNSIGNED);
        else if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            if (pos->type == TokenType::Number)
                ++pos;
            if (pos->type != TokenType::ClosingRoundBracket)
               return false;
            ++pos;
            if (ParserKeyword(Keyword::SIGNED).ignore(pos, expected))
                type_name_suffix = toStringView(Keyword::SIGNED);
            else if (ParserKeyword(Keyword::UNSIGNED).ignore(pos, expected))
                type_name_suffix = toStringView(Keyword::UNSIGNED);
        }

    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;

    /// skip trailing comma in types, e.g. Tuple(Int, String,)
    if (pos->type == TokenType::Comma)
    {
        Expected test_expected;
        auto test_pos = pos;
        ++test_pos;
        if (ParserToken(TokenType::ClosingRoundBracket).ignore(test_pos, test_expected))
        { // the end of the type definition was reached and there was a trailing comma
            ++pos;
        }
    }

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = type_name;
    function_node->no_empty_args = true;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = function_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ParserList args_parser(std::make_unique<ParserDataTypeArgument>(type_name), std::make_unique<ParserToken>(TokenType::Comma));
    ASTPtr expr_list_args;

    if (!args_parser.parse(pos, expr_list_args, expected))
        return false;
    if (pos->type == TokenType::Comma)
        // ignore trailing comma inside Nested structures like Tuple(Int, Tuple(Int, String),)
        ++pos;
    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = function_node;
    return true;
}

}
