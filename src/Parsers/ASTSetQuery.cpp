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

void ASTSetQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (is_standalone)
        format.ostr << (format.hilite ? hilite_keyword : "") << "SET " << (format.hilite ? hilite_none : "");

    bool first = true;

    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (!first)
            format.ostr << ", ";
        else
            first = false;

        formatSettingName(it->name, format.ostr);
        format.ostr << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }

    for (auto it = query_parameters.begin(); it != query_parameters.end(); ++it)
    {
        if (!first)
            format.ostr << ", ";
        else
            first = false;

        formatSettingName(QUERY_PARAMETER_NAME_PREFIX + it->first, format.ostr);
        format.ostr << " = " << it->second;
    }
}

}
