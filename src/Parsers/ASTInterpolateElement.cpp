#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTInterpolateElement::clone() const
{
    auto clone = make_intrusive<ASTInterpolateElement>(*this);
    clone->expr = clone->expr->clone();
    clone->children.clear();
    clone->children.push_back(clone->expr);
    return clone;
}

void ASTInterpolateElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `column` is part of `getID`. A double-quoted target is pinned to exact matching under `standard`
    /// name matching; mixed in only when set, so the hash of an unquoted target stays unchanged.
    if (column_quote == IdentifierPartQuote::DoubleQuoted)
        hash_state.update(column_quote);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
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
    if (column.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate element must have a non-empty column during AST JSON deserialization");

    auto child = r.readChild("expr");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate element must have an expression during AST JSON deserialization");
    expr = child;
    children.push_back(expr);
}

}
