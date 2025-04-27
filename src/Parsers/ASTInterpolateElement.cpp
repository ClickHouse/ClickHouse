#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTInterpolateElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
        ostr << column << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        expr->format(ostr, settings, state, frame);
}

}
