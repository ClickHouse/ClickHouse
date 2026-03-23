#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
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

    /// If the expression has an alias, we need to wrap it in parentheses
    /// to avoid ambiguity with double AS: `col AS expr AS alias` is not parseable,
    /// but `col AS (expr AS alias)` is.
    bool need_parens = !expr->tryGetAlias().empty();
    if (need_parens)
        ostr << '(';
    expr->format(ostr, settings, state, frame);
    if (need_parens)
        ostr << ')';
}

void ASTInterpolateElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "InterpolateElement");
    w.writeString("column", column);
    w.writeChild("expr", expr);
}

void ASTInterpolateElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    column = r.getString("column");

    auto child = r.readChild("expr");
    if (child)
    {
        expr = child;
        children.push_back(expr);
    }
}

}
