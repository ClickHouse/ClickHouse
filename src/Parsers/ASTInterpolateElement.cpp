#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTInterpolateElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
        ostr << column << " AS ";
        expr->format(ostr, settings, state, frame);
}

}
