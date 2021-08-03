#include <Parsers/ASTSetQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/SipHash.h>
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

    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (it != changes.begin())
            format.ostr << ", ";

        formatSettingName(it->name, format.ostr);
        format.ostr << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }
}

}
