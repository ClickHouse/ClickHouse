#include <Parsers/ASTUDTReference.h>

#include <IO/Operators.h>
#include <Common/SipHash.h>

#include <string_view>


namespace DB
{

String ASTUDTReference::getID(char delim) const
{
    return "UDTReference" + (delim + database_name) + (delim + type_name);
}

ASTPtr ASTUDTReference::clone() const
{
    auto result = make_intrusive<ASTUDTReference>(*this);
    const auto & arguments = getArguments();
    result->children.clear();

    if (arguments)
        result->children.push_back(arguments->clone());

    return result;
}

ASTPtr ASTUDTReference::getArguments() const
{
    if (!children.empty())
        return children[0];
    return nullptr;
}

void ASTUDTReference::resetArguments()
{
    children.clear();
}

void ASTUDTReference::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    static constexpr std::string_view domain = "ASTUDTReference";
    hash_state.update(domain.size());
    hash_state.update(domain.data(), domain.size());
    hash_state.update(database_name.size());
    hash_state.update(database_name);
    hash_state.update(type_name.size());
    hash_state.update(type_name);
    /// Children are hashed automatically.
}

void ASTUDTReference::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!database_name.empty())
    {
        settings.writeIdentifier(ostr, database_name, /*ambiguous=*/false);
        ostr << '.';
    }
    settings.writeIdentifier(ostr, type_name, /*ambiguous=*/false);

    const auto & arguments = getArguments();
    if (arguments && !arguments->children.empty())
    {
        ostr << '(';
        frame.expression_list_prepend_whitespace = false;
        arguments->format(ostr, settings, state, frame);
        ostr << ')';
    }
}

}
