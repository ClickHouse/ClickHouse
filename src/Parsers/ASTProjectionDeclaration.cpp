#include <IO/Operators.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTProjectionDeclaration::clone() const
{
    auto res = std::make_shared<ASTProjectionDeclaration>();
    res->name = name;
    if (query)
        res->set(res->query, query->clone());
    return res;
}


void ASTProjectionDeclaration::formatImpl(const FormattingBuffer & out) const
{
    out.ostr << backQuoteIfNeed(name);
    out.nlOrWs();
    out.writeIndent();
    out.ostr << "(";
    out.nlOrNothing();
    query->formatImpl(out.copyWithoutNeedParensAndWithExtraIndent());
    out.nlOrNothing();
    out.writeIndent();
    out.ostr << ")";
}

}
