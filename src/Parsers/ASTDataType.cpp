#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Core/Defines.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <limits>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool equalsCaseInsensitiveString(std::string_view lhs, std::string_view rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
        if (!equalsCaseInsensitive(lhs[i], rhs[i]))
            return false;
    return true;
}

bool containsCaseInsensitive(std::string_view haystack, std::string_view needle)
{
    if (haystack.size() < needle.size())
        return false;
    for (size_t i = 0; i + needle.size() <= haystack.size(); ++i)
        if (equalsCaseInsensitiveString(haystack.substr(i, needle.size()), needle))
            return true;
    return false;
}

bool isBareUUIDTypeName(const String & name)
{
    return equalsCaseInsensitiveString(name, "uuid");
}

bool substituteBareUUIDInPlace(IAST & ast);

/// Position of the type name argument of a function, or `last_type_name_argument` when the type name is
/// always the last argument (as for the `JSONExtract` family, which takes a variable number of path arguments).
constexpr size_t last_type_name_argument = std::numeric_limits<size_t>::max();

struct TypeNameArgument
{
    std::string_view function_name;
    size_t argument_index;
};

/** Functions that declare their result type through a data type name in a string literal argument.
  *
  * The parser canonicalizes `CAST(expr AS T)` and `expr::T` into `CAST(expr, 'T')`, so inside persisted
  * expressions (column defaults, `ORDER BY` / `PARTITION BY` / TTL expressions, view and `AS SELECT`
  * definitions) a type name written that way survives only as such a literal. The other functions take a type
  * name as a string in the query text to begin with. All of them resolve a bare `UUID` through
  * `DataTypeFactory`, so `uuid_type_version` has to be materialized into them as well - otherwise the setting
  * would be silently ignored there.
  *
  * Deliberately absent are the functions that do not declare a type but look up an existing one by name -
  * `variantElement`, `dynamicElement`, `getTypeSerializationStreams`. There the name has to match a type that
  * is already present in the data, which may well be the historical `UUID`; rewriting it could silently turn
  * such an expression into `NULL`s instead of failing loudly.
  */
constexpr TypeNameArgument type_name_arguments[]
{
    {"CAST", 1},
    {"_CAST", 1},
    {"accurateCast", 1},
    {"accurateCastOrNull", 1},
    {"accurateCastOrDefault", 1},
    {"reinterpret", 1},
    {"defaultValueOfTypeName", 0},
    {"JSONExtract", last_type_name_argument},
    {"JSONExtractKeysAndValues", last_type_name_argument},
    {"JSONExtractCaseInsensitive", last_type_name_argument},
    {"JSONExtractKeysAndValuesCaseInsensitive", last_type_name_argument},
};

bool substituteBareUUIDInTypeNameLiteral(ASTFunction & function)
{
    if (!function.arguments)
        return false;

    const auto & arguments = function.arguments->children;
    std::optional<size_t> type_argument_index;

    for (const auto & candidate : type_name_arguments)
    {
        if (!equalsCaseInsensitiveString(function.name, candidate.function_name))
            continue;

        if (candidate.argument_index == last_type_name_argument)
        {
            /// The first argument is the value to extract from, so the type can only be a later argument.
            if (arguments.size() >= 2)
                type_argument_index = arguments.size() - 1;
        }
        else if (candidate.argument_index < arguments.size())
        {
            type_argument_index = candidate.argument_index;
        }

        break;
    }

    if (!type_argument_index)
        return false;

    auto * type_literal = arguments[*type_argument_index]->as<ASTLiteral>();
    if (!type_literal || type_literal->value.getType() != Field::Types::String)
        return false;

    const auto & type_name = type_literal->value.safeGet<String>();
    if (!containsCaseInsensitive(type_name, "uuid"))
        return false;

    Tokens tokens(type_name.data(), type_name.data() + type_name.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    ASTPtr type_ast;

    /// An unparsable type string is left as is: it is not a bare `UUID`, and resolving it will fail later anyway.
    if (!ParserDataType{}.parse(pos, type_ast, expected) || !type_ast || pos->type != TokenType::EndOfStream)
        return false;

    if (!substituteBareUUIDInPlace(*type_ast))
        return false;

    type_literal->value = type_ast->formatWithSecretsOneLine();
    return true;
}

bool substituteBareUUIDInPlace(IAST & ast)
{
    bool substituted = false;

    if (auto * data_type = ast.as<ASTDataType>(); data_type && isBareUUIDTypeName(data_type->name))
    {
        data_type->name = "UUID2";
        substituted = true;
    }

    if (auto * function = ast.as<ASTFunction>())
        substituted |= substituteBareUUIDInTypeNameLiteral(*function);

    for (const auto & child : ast.children)
        if (child)
            substituted |= substituteBareUUIDInPlace(*child);

    return substituted;
}

}

ASTPtr applyUUIDTypeVersion(const ASTPtr & type_ast, UInt64 uuid_type_version)
{
    if (uuid_type_version != 2 || !type_ast)
        return type_ast;

    auto cloned = type_ast->clone();
    if (substituteBareUUIDInPlace(*cloned))
        return cloned;
    return type_ast;
}

bool applyUUIDTypeVersionInPlace(IAST & ast, UInt64 uuid_type_version)
{
    if (uuid_type_version != 2)
        return false;

    return substituteBareUUIDInPlace(ast);
}

String ASTDataType::getID(char delim) const
{
    return "DataType" + (delim + name);
}

ASTPtr ASTDataType::clone() const
{
    auto res = make_intrusive<ASTDataType>(*this);
    const auto & arguments = getArguments();
    res->children.clear();

    if (arguments)
        res->children.push_back(arguments->clone());

    return res;
}

ASTPtr ASTDataType::getArguments() const
{
    if (!children.empty())
        return children[0];
    return nullptr;
}

void ASTDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);
}

void ASTDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for ASTDataType");

    /// `arguments` is the `ASTExpressionList` produced by `ParserDataType`. `formatImpl` only prints
    /// the `(...)` when this child has its own `children`, so a non-list node here would be silently
    /// dropped (e.g. `Nullable(UInt8)` formatting as bare `Nullable`). Reject it at the JSON boundary.
    auto args = r.readChildOfType<ASTExpressionList>("arguments");
    if (args)
        children.push_back(args);
}

void ASTDataType::resetArguments()
{
    children.clear();
}

void ASTDataType::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    /// Children are hashed automatically.
}

void ASTDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << name;

    const auto & arguments = getArguments();
    if (arguments && !arguments->children.empty())
    {
        ostr << '(';

        if (!settings.one_line && settings.print_pretty_type_names && name == "Tuple")
        {
            ++frame.indent;
            std::string indent_str = settings.one_line ? "" : "\n" + std::string(4 * frame.indent, ' ');
            for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
            {
                if (i != 0)
                    ostr << ',';
                ostr << indent_str;
                arguments->children[i]->format(ostr, settings, state, frame);
            }
        }
        else
        {
            frame.expression_list_prepend_whitespace = false;
            arguments->format(ostr, settings, state, frame);
        }

        ostr << ')';
    }
}

}
