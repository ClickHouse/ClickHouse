#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "ASTColumnsClause.h"

namespace DB
{

ASTPtr ASTColumnsClause::clone() const
{
    auto clone = std::make_shared<ASTColumnsClause>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTColumnsClause::appendColumnName(WriteBuffer & ostr) const { writeString(originalPattern, ostr); }

void ASTColumnsClause::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << " '" << originalPattern << "'";
}

}
