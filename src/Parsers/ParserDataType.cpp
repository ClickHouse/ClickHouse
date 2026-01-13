#include <Parsers/ParserDataType.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTDecimalDataType.h>
#include <Parsers/ASTDateTime64DataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTFixedStringDataType.h>
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

/// Check if type name is an Enum type
bool isEnumType(const String & type_name_upper)
{
    return type_name_upper == "ENUM" || type_name_upper == "ENUM8" || type_name_upper == "ENUM16";
}

/// Check if type name is a Decimal type
bool isDecimalType(const String & type_name_upper)
{
    return type_name_upper == "DECIMAL" || type_name_upper == "DECIMAL32"
        || type_name_upper == "DECIMAL64" || type_name_upper == "DECIMAL128"
        || type_name_upper == "DECIMAL256";
}

/// Check if type name is DateTime64
bool isDateTime64Type(const String & type_name_upper)
{
    return type_name_upper == "DATETIME64";
}

/// Check if type name is FixedString
bool isFixedStringType(const String & type_name_upper)
{
    return type_name_upper == "FIXEDSTRING";
}

/// Parse an unsigned integer from current token
bool parseUnsignedInteger(IParser::Pos & pos, UInt64 & value, Expected & expected)
{
    if (pos->type != TokenType::Number)
    {
        expected.add(pos, "number");
        return false;
    }

    ReadBufferFromMemory in(pos->begin, pos->size());
    if (!tryReadIntText(value, in) || in.count() != pos->size())
    {
        expected.add(pos, "unsigned integer");
        return false;
    }
    ++pos;
    return true;
}

/// Parse enum values directly into the vector without creating ASTLiteral nodes.
/// Format: 'name1' = value1, 'name2' = value2, ... or just 'name1', 'name2', ...
/// Returns true on success.
bool parseEnumValues(
    IParser::Pos & pos,
    std::vector<std::pair<String, Int64>> & values,
    Expected & expected)
{
    bool first_element = true;
    Int64 auto_value = 1;
    bool has_explicit_values = false;
    bool has_auto_values = false;

    while (true)
    {
        if (!first_element)
        {
            if (pos->type != TokenType::Comma)
                break;
            ++pos;
        }
        first_element = false;

        /// Parse the string name
        if (pos->type != TokenType::StringLiteral)
        {
            expected.add(pos, "string literal for enum element name");
            return false;
        }

        String elem_name;
        ReadBufferFromMemory in(pos->begin, pos->size());
        try
        {
            readQuotedStringWithSQLStyle(elem_name, in);
        }
        catch (const Exception &)
        {
            expected.add(pos, "valid string literal");
            return false;
        }

        if (in.count() != pos->size())
        {
            expected.add(pos, "string literal");
            return false;
        }
        ++pos;

        Int64 elem_value;

        /// Check for '=' and value
        if (pos->type == TokenType::Equals)
        {
            ++pos;
            has_explicit_values = true;

            /// Parse the numeric value (can be negative)
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
            {
                expected.add(pos, "integer value");
                return false;
            }
            ++pos;

            elem_value = negative ? -static_cast<Int64>(abs_value) : static_cast<Int64>(abs_value);

            /// Update auto_value for subsequent auto-assigned elements
            auto_value = elem_value;
        }
        else
        {
            /// Auto-assign value
            has_auto_values = true;
            elem_value = auto_value;
        }

        values.emplace_back(elem_name, elem_value);
        ++auto_value;
    }

    /// Check that we don't mix explicit and auto values (except for the first element which sets the base)
    if (has_explicit_values && has_auto_values && values.size() > 1)
    {
        /// This is actually allowed - first element can have explicit value, rest can be auto
        /// The rule is: either all explicit OR first explicit then all auto OR all auto
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
        auto argument = std::make_shared<ASTObjectTypeArgument>();

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

        auto name_and_type = std::make_shared<ASTObjectTypedPathArgument>();
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
    /// to avoid creating hundreds of ASTLiteral nodes for large enums
    if (isEnumType(type_name_upper))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
        {
            expected.add(pos, "opening bracket for Enum values");
            return false;
        }
        ++pos;

        auto enum_node = std::make_shared<ASTEnumDataType>();
        enum_node->name = type_name;

        if (!parseEnumValues(pos, enum_node->values, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
        {
            expected.add(pos, "closing bracket");
            return false;
        }
        ++pos;

        node = enum_node;
        return true;
    }

    /// Handle Decimal types specially
    if (isDecimalType(type_name_upper))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
        {
            expected.add(pos, "opening bracket for Decimal parameters");
            return false;
        }
        ++pos;

        auto decimal_node = std::make_shared<ASTDecimalDataType>();
        decimal_node->name = type_name;

        UInt64 first_param = 0;
        if (!parseUnsignedInteger(pos, first_param, expected))
            return false;

        /// Decimal(precision, scale) has two parameters
        /// Decimal32/64/128/256(scale) has one parameter
        if (type_name_upper == "DECIMAL")
        {
            decimal_node->precision = static_cast<UInt32>(first_param);

            if (pos->type == TokenType::Comma)
            {
                ++pos;
                UInt64 scale = 0;
                if (!parseUnsignedInteger(pos, scale, expected))
                    return false;
                decimal_node->scale = static_cast<UInt32>(scale);
            }
            else
            {
                /// Decimal(P) without scale means scale = 0
                decimal_node->scale = 0;
            }
        }
        else
        {
            /// Decimal32/64/128/256 - first param is scale
            decimal_node->scale = static_cast<UInt32>(first_param);
            /// Set precision based on type
            if (type_name_upper == "DECIMAL32")
                decimal_node->precision = 9;
            else if (type_name_upper == "DECIMAL64")
                decimal_node->precision = 18;
            else if (type_name_upper == "DECIMAL128")
                decimal_node->precision = 38;
            else if (type_name_upper == "DECIMAL256")
                decimal_node->precision = 76;
        }

        if (pos->type != TokenType::ClosingRoundBracket)
        {
            expected.add(pos, "closing bracket");
            return false;
        }
        ++pos;

        node = decimal_node;
        return true;
    }

    /// Handle DateTime64 specially
    if (isDateTime64Type(type_name_upper))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
        {
            expected.add(pos, "opening bracket for DateTime64 parameters");
            return false;
        }
        ++pos;

        auto dt64_node = std::make_shared<ASTDateTime64DataType>();
        dt64_node->name = type_name;

        UInt64 precision = 0;
        if (!parseUnsignedInteger(pos, precision, expected))
            return false;
        dt64_node->precision = static_cast<UInt32>(precision);

        /// Optional timezone parameter
        if (pos->type == TokenType::Comma)
        {
            ++pos;

            if (pos->type != TokenType::StringLiteral)
            {
                expected.add(pos, "timezone string");
                return false;
            }

            String timezone;
            ReadBufferFromMemory in(pos->begin, pos->size());
            try
            {
                readQuotedStringWithSQLStyle(timezone, in);
            }
            catch (const Exception &)
            {
                expected.add(pos, "valid timezone string");
                return false;
            }
            ++pos;
            dt64_node->timezone = timezone;
        }

        if (pos->type != TokenType::ClosingRoundBracket)
        {
            expected.add(pos, "closing bracket");
            return false;
        }
        ++pos;

        node = dt64_node;
        return true;
    }

    /// Handle FixedString specially
    if (isFixedStringType(type_name_upper))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
        {
            expected.add(pos, "opening bracket for FixedString parameter");
            return false;
        }
        ++pos;

        auto fs_node = std::make_shared<ASTFixedStringDataType>();
        fs_node->name = type_name;

        UInt64 n = 0;
        if (!parseUnsignedInteger(pos, n, expected))
            return false;
        fs_node->n = n;

        if (pos->type != TokenType::ClosingRoundBracket)
        {
            expected.add(pos, "closing bracket");
            return false;
        }
        ++pos;

        node = fs_node;
        return true;
    }

    auto data_type_node = std::make_shared<ASTDataType>();
    data_type_node->name = type_name;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = data_type_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ASTPtr expr_list_args = std::make_shared<ASTExpressionList>();

    /// Allow mixed lists of nested and normal types.
    /// Parameters are either:
    /// - Nested table element;
    /// - Tuple element
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

    data_type_node->arguments = expr_list_args;
    data_type_node->children.push_back(data_type_node->arguments);

    node = data_type_node;
    return true;
}

}
