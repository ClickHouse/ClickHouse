#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWithElement::clone() const
{
    const auto res = std::make_shared<ASTWithElement>(*this);
    res->children.clear();
    res->subquery = subquery->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::formatImpl(const FormattingBuffer & out) const
{
    out.writeAlias(name);
    out.writeKeyword(" AS");
    out.nlOrWs();
    out.writeIndent();
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(out);
}

}
