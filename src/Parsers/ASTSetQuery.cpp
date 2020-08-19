#include <Parsers/ASTSetQuery.h>
#include <Parsers/formatSettingName.h>


namespace DB
{
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
