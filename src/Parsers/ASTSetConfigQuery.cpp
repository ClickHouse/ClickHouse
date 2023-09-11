#include <Parsers/ASTSetConfigQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTSetQuery.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

void ASTSetConfigQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "SET CONFIG " << (format.hilite ? hilite_none : "");
    if (is_global)
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "GLOBAL " << (format.hilite ? IAST::hilite_none : "");
    formatSettingName(name, format.ostr);
    if (is_none)
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " NONE" << (format.hilite ? IAST::hilite_none : "");
    else
        format.ostr << " = " << applyVisitor(FieldVisitorToString(), value);
}

}
