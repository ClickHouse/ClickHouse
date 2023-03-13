#include <Parsers/ASTSetQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTSetQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    for (const auto & change : changes)
    {
        hash_state.update(change.name.size());
        hash_state.update(change.name);
        applyVisitor(FieldVisitorHash(hash_state), change.value);
    }
}

void ASTSetQuery::formatImpl(const FormattingBuffer & out) const
{
    if (is_standalone)
        out.writeKeyword("SET ");

    bool first = true;

    for (const auto & change : changes)
    {
        if (!first)
            out.ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, out.ostr);
        CustomType custom;
        if (!out.shouldShowSecrets() && change.value.tryGet<CustomType>(custom) && custom.isSecret())
            out.ostr << " = " << custom.toString(false);
        else
            out.ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
    }

    for (const auto & setting_name : default_settings)
    {
        if (!first)
            out.ostr << ", ";
        else
            first = false;

        formatSettingName(setting_name, out.ostr);
        out.ostr << " = DEFAULT";
    }

    for (const auto & [name, value] : query_parameters)
    {
        if (!first)
            out.ostr << ", ";
        else
            first = false;

        formatSettingName(QUERY_PARAMETER_NAME_PREFIX + name, out.ostr);
        out.ostr << " = " << value;
    }
}

}
