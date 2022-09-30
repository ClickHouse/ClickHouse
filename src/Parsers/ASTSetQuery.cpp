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

void ASTSetQuery::formatImpl(const FormatSettings & format, FormatState & state, FormatStateStacked stacked) const
{
    if (is_standalone)
        format.ostr << (format.hilite ? hilite_keyword : "") << "SET " << (format.hilite ? hilite_none : "");

    bool first = true;

    for (const auto & change : changes)
    {
        if (!first)
            format.ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, format.ostr);
        if (it->value_ast)
        {
            format.ostr << " = ";
            it->value_ast->formatImpl(format, state, stacked);
        }
        else
            format.ostr << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }

    for (const auto & [name, value] : query_parameters)
    {
        if (!first)
            format.ostr << ", ";
        else
            first = false;

        formatSettingName(QUERY_PARAMETER_NAME_PREFIX + name, format.ostr);
        format.ostr << " = " << value;
    }
}

}
