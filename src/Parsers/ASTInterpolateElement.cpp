#include <Parsers/ASTInterpolateElement.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTInterpolateElement::clone() const
{
    auto clone = make_intrusive<ASTInterpolateElement>(*this);
    clone->expr = clone->expr->clone();
    clone->children.clear();
    clone->children.push_back(clone->expr);
    return clone;
}


void ASTInterpolateElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, column, /*ambiguous=*/true);
    ostr << " AS ";

    /// If the expression has an alias, it must be wrapped in parentheses
    /// to avoid ambiguity with double AS: `col AS expr AS alias` is not parseable,
    /// but `col AS (expr AS alias)` is. Setting `need_parens` makes the generic
    /// aliased-expression handling produce the wrap.
    frame.need_parens = !expr->tryGetAlias().empty();
    expr->format(ostr, settings, state, frame);
}

}
