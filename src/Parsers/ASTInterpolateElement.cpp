#include <Parsers/ASTInterpolateElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTInterpolateElement::clone() const
{
    auto clone = std::make_shared<ASTInterpolateElement>(*this);
    clone->expr = clone->expr->clone();
    clone->children.clear();
    clone->children.push_back(clone->expr);
    return clone;
}


void ASTInterpolateElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << column << " AS ";
    expr->format(ostr, settings, state, frame);
}

}
