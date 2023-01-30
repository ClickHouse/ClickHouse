#include <Columns/Collator.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTInterpolateElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << column;
    settings.writeKeyword(" AS ");
        expr->formatImpl(settings, state, frame);
}

}
