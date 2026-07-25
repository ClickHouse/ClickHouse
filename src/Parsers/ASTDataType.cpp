#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/TokenIterator.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Core/Defines.h>
#include <IO/Operators.h>


namespace DB
{

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

/// The parser canonicalizes `CAST(expr AS T)` and `expr::T` into `CAST(expr, 'T')`, so inside persisted
/// expressions (column defaults, view and `AS SELECT` definitions) a type name survives only as a string
/// literal argument of a cast function. Rewrite a bare `UUID` inside those literals as well.
bool substituteBareUUIDInCastTypeLiteral(ASTFunction & function)
{
    static constexpr std::string_view cast_function_names[] = {"CAST", "_CAST", "accurateCast", "accurateCastOrNull", "accurateCastOrDefault"};

    bool is_cast_function = false;
    for (const auto & cast_function_name : cast_function_names)
        is_cast_function |= equalsCaseInsensitiveString(function.name, cast_function_name);
    if (!is_cast_function)
        return false;

    if (!function.arguments || function.arguments->children.size() < 2)
        return false;

    auto * type_literal = function.arguments->children[1]->as<ASTLiteral>();
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
        substituted |= substituteBareUUIDInCastTypeLiteral(*function);

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
