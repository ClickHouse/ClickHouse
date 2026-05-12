#include <Parsers/ParserDataType.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Common/StringUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace
{

bool isEnumType(const String & type_name_upper)
{
    return type_name_upper == "ENUM" || type_name_upper == "ENUM8" || type_name_upper == "ENUM16";
}

/// Parse enum values directly into the vector without creating ASTLiteral nodes.
/// Format: 'name1' = value1, 'name2' = value2, ...
/// Only handles fully explicit enums (all values must have = number).
/// Returns false to fall back to generic parser for auto-assigned values or special literals.
bool parseEnumValues(
    IParser::Pos & pos,
    std::vector<std::pair<String, Int64>> & values,
    Expected & expected)
{
    bool first_element = true;

    while (true)
    {
        if (!first_element)
        {
            if (pos->type != TokenType::Comma)
                break;
            ++pos;
        }
        first_element = false;

        if (pos->type != TokenType::StringLiteral)
        {
            expected.add(pos, "string literal for enum element name");
            return false;
        }

        /// Check for prefixed string literals like b'...' or x'...' - fall back to generic parser
        char first_char = *pos->begin;
        if (first_char == 'b' || first_char == 'B' || first_char == 'x' || first_char == 'X')
            return false;

        String elem_name;
        ReadBufferFromMemory in(pos->begin, pos->size());
        if (!tryReadQuotedStringWithSQLStyle(elem_name, in) || in.count() != pos->size())
            return false;
        ++pos;

        /// Must have explicit value - if not, fall back to generic parser for auto-assignment
        if (pos->type != TokenType::Equals)
            return false;
        ++pos;

        bool negative = false;
        if (pos->type == TokenType::Minus)
        {
            negative = true;
            ++pos;
        }

        if (pos->type != TokenType::Number)
        {
            expected.add(pos, "number for enum element value");
            return false;
        }

        UInt64 abs_value = 0;
        ReadBufferFromMemory num_in(pos->begin, pos->size());
        if (!tryReadIntText(abs_value, num_in) || num_in.count() != pos->size())
            return false;
        ++pos;

        Int64 elem_value = negative ? -static_cast<Int64>(abs_value) : static_cast<Int64>(abs_value);
        values.emplace_back(elem_name, elem_value);
    }

    return !values.empty();
}

/// Parser of Dynamic type argument: Dynamic(max_types=N)
class DynamicArgumentParser : public IParserBase
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

        node = makeASTOperator("equals", identifier, number);
        return true;
    }
};

/// Parser of Object type argument. For example: JSON(some_parameter=N, some.path SomeType, SKIP skip.path, ...)
class ObjectArgumentParser : public IParserBase
{
private:
    const char * getName() const override { return "JSON data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto argument = make_intrusive<ASTObjectTypeArgument>();

        /// SKIP arguments
        if (ParserKeyword(Keyword::SKIP).ignore(pos))
        {
            /// SKIP REGEXP '<some_regexp>'
            if (ParserKeyword(Keyword::REGEXP).ignore(pos))
            {
                ParserStringLiteral literal_parser;
                ASTPtr literal;
                if (!literal_parser.parse(pos, literal, expected))
                    return false;
                argument->skip_path_regexp = literal;
                argument->children.push_back(argument->skip_path_regexp);
            }
            /// SKIP some.path
            else
            {
                ParserCompoundIdentifier compound_identifier_parser;
                ASTPtr compound_identifier;
                if (!compound_identifier_parser.parse(pos, compound_identifier, expected))
                    return false;

                argument->skip_path = compound_identifier;
                argument->children.push_back(argument->skip_path);
            }

            node = argument;
            return true;
        }

        ParserCompoundIdentifier compound_identifier_parser;
        ASTPtr identifier;
        if (!compound_identifier_parser.parse(pos, identifier, expected))
            return false;

        /// some_parameter=N
        if (pos->type == TokenType::Equals)
        {
            ++pos;
            ASTPtr number;
            ParserNumber number_parser;
            if (!number_parser.parse(pos, number, expected))
                return false;

            argument->parameter = makeASTOperator("equals", identifier, number);
            argument->children.push_back(argument->parameter);
            node = argument;
            return true;
        }

        ParserDataType type_parser;
        ASTPtr type;
        if (!type_parser.parse(pos, type, expected))
            return false;

        auto name_and_type = make_intrusive<ASTObjectTypedPathArgument>();
        name_and_type->path = getIdentifierName(identifier);
        name_and_type->type = type;
        name_and_type->children.push_back(name_and_type->type);
        argument->path_with_type = name_and_type;
        argument->children.push_back(argument->path_with_type);
        node = argument;
        return true;
    }
};

}

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
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
    else if (type_name_upper.contains("INT"))
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

    /// Handle Enum types specially - parse directly into ASTEnumDataType
    /// to avoid creating hundreds of ASTLiteral nodes for large enums.
    /// Only handles fully explicit enums; falls back to generic parser for auto-assigned values.
    if (isEnumType(type_name_upper) && pos->type == TokenType::OpeningRoundBracket)
    {
        auto saved_pos = pos;
        ++pos;

        auto enum_node = make_intrusive<ASTEnumDataType>();
        enum_node->name = type_name;

        if (parseEnumValues(pos, enum_node->values, expected) && pos->type == TokenType::ClosingRoundBracket)
        {
            ++pos;
            enum_node->values.shrink_to_fit();
            node = enum_node;
            return true;
        }
        pos = saved_pos;
    }

    /// Handle Tuple types specially - parse directly into ASTTupleDataType
    /// to avoid creating ASTNameTypePair nodes for each named element.
    if (type_name == "Tuple" && pos->type == TokenType::OpeningRoundBracket)
    {
        auto saved_pos = pos;
        ++pos;

        auto tuple_node = make_intrusive<ASTTupleDataType>();
        tuple_node->name = type_name;
        auto arguments = make_intrusive<ASTExpressionList>();
        tuple_node->children.push_back(arguments);

        bool has_named_elements = false;
        Strings element_names_tmp;
        bool first_element = true;

        while (true)
        {
            if (!first_element)
            {
                if (pos->type == TokenType::Comma)
                    ++pos;
                else
                    break;
            }
            first_element = false;

            /// Try to parse: identifier Type (named element)
            /// or just: Type (unnamed element)
            ParserIdentifier identifier_parser;
            ParserDataType type_parser;
            ASTPtr identifier_node;
            ASTPtr type_node;

            auto element_pos = pos;
            if (identifier_parser.parse(pos, identifier_node, expected) && type_parser.parse(pos, type_node, expected))
            {
                /// Named element: name Type
                String elem_name;
                tryGetIdentifierNameInto(identifier_node, elem_name);
                element_names_tmp.push_back(elem_name);
                arguments->children.push_back(type_node);
                has_named_elements = true;
            }
            else
            {
                /// Try just Type (unnamed element)
                pos = element_pos;
                if (type_parser.parse(pos, type_node, expected))
                {
                    /// Empty placeholder needed to detect mixed named/unnamed tuples.
                    /// The factory validates that all names are non-empty when element_names is set.
                    element_names_tmp.push_back("");
                    arguments->children.push_back(type_node);
                }
                else
                {
                    /// Could not parse element
                    break;
                }
            }
        }

        if (pos->type == TokenType::ClosingRoundBracket && !arguments->children.empty())
        {
            ++pos;
            /// Only store element_names if tuple has any named elements
            if (has_named_elements)
            {
                element_names_tmp.shrink_to_fit();
                tuple_node->element_names = std::move(element_names_tmp);
            }
            arguments->children.shrink_to_fit();
            node = tuple_node;
            return true;
        }

        /// Fall back to generic parser
        pos = saved_pos;
    }

    auto data_type_node = make_intrusive<ASTDataType>();
    data_type_node->name = type_name;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = data_type_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ASTPtr expr_list_args = make_intrusive<ASTExpressionList>();

    /// Allow mixed lists of nested and normal types.
    /// Parameters are either:
    /// - Nested table element;
    /// - Tuple element
    /// - Enum element in form of 'a' = 1;
    /// - literal;
    /// - Dynamic type argument;
    /// - JSON type argument;
    /// - another data type (or identifier);

    size_t arg_num = 0;
    bool have_version_of_aggregate_function = false;
    while (true)
    {
        if (arg_num > 0)
        {
            if (pos->type == TokenType::Comma)
                ++pos;
            else
                break;
        }

        ASTPtr arg;
        if (type_name == "Dynamic")
        {
            DynamicArgumentParser parser;
            parser.parse(pos, arg, expected);
        }
        else if (boost::to_lower_copy(type_name) == "json")
        {
            ObjectArgumentParser parser;
            parser.parse(pos, arg, expected);
        }
        else if (type_name == "Nested")
        {
            ParserNameTypePair name_and_type_parser;
            name_and_type_parser.parse(pos, arg, expected);
        }
        else if (type_name == "Tuple")
        {
            ParserNameTypePair name_and_type_parser;
            ParserDataType only_type_parser;
            name_and_type_parser.parse(pos, arg, expected) || only_type_parser.parse(pos, arg, expected);
        }
        else if (type_name == "AggregateFunction" || type_name == "SimpleAggregateFunction")
        {
            /// This is less trivial.
            /// The first optional argument for AggregateFunction is a numeric literal, defining the version.
            /// The next argument is the function name, optionally with parameters.
            /// Subsequent arguments are data types.

            if (arg_num == 0 && type_name == "AggregateFunction")
            {
                ParserUnsignedInteger version_parser;
                if (version_parser.parse(pos, arg, expected))
                {
                    have_version_of_aggregate_function = true;
                    expr_list_args->children.emplace_back(std::move(arg));
                    ++arg_num;
                    continue;
                }
            }

            if (arg_num == (have_version_of_aggregate_function ? 1 : 0))
            {
                ParserFunction function_parser;
                ParserIdentifier identifier_parser;
                function_parser.parse(pos, arg, expected)
                    || identifier_parser.parse(pos, arg, expected);
            }
            else
            {
                ParserDataType data_type_parser;
                data_type_parser.parse(pos, arg, expected);
            }
        }
        else
        {
            ParserDataType data_type_parser;
            ParserAllCollectionsOfLiterals literal_parser(false);

            const char * operators[] = {"=", "equals", nullptr};
            ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>());

            enum_parser.parse(pos, arg, expected)
               || literal_parser.parse(pos, arg, expected)
               || data_type_parser.parse(pos, arg, expected);
        }

        if (!arg)
            break;

        expr_list_args->children.emplace_back(std::move(arg));
        ++arg_num;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    data_type_node->children.push_back(expr_list_args);

    node = data_type_node;
    return true;
}

}
