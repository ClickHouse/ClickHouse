#include <Parsers/ASTDataType.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <IO/Operators.h>


namespace DB
{

namespace
{

bool isBareUUIDTypeName(const String & name)
{
    static constexpr std::string_view uuid = "uuid";
    if (name.size() != uuid.size())
        return false;
    for (size_t i = 0; i < name.size(); ++i)
        if (!equalsCaseInsensitive(name[i], uuid[i]))
            return false;
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
