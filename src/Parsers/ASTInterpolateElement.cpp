#include <Columns/Collator.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTInterpolateElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
        settings.ostr << column << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        expr->formatImpl(settings, state, frame);
}

}
