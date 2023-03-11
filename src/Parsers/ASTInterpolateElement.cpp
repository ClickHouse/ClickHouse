#include <Columns/Collator.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTInterpolateElement::formatImpl(const FormattingBuffer & out) const
{
    out.ostr << column;
    out.writeKeyword(" AS ");
        expr->formatImpl(out);
}

}
