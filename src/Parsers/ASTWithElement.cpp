#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTWithElement::clone() const
{
    const auto res = make_intrusive<ASTWithElement>(*this);
    res->children.clear();
    res->subquery = subquery->clone();
    if (aliases)
        res->aliases = aliases->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "WithElement");
    w.writeString("name", name);
    if (is_materialized)
        w.writeBool("is_materialized", true);
    w.writeChild("subquery", subquery);
    w.writeChild("aliases", aliases);
}

void ASTWithElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty 'name' during AST JSON deserialization");
    is_materialized = r.getBool("is_materialized");

    /// The parser produces an `ASTSubquery` here (`ParserWithElement` uses `ParserSubquery`), and the
    /// analyzer relies on exactly that: `QueryTreeBuilder::buildExpression` does
    /// `with_element->subquery->as<ASTSubquery &>().children.at(0)`. A looser `ASTWithAlias` (e.g. an
    /// `ASTFunction` or `ASTIdentifier`, which also satisfy `formatImpl`'s `dynamic_cast<ASTWithAlias &>`)
    /// would pass formatting but reach that hard downcast as an internal error. Require an `ASTSubquery`.
    subquery = r.readChildOfType<ASTSubquery>("subquery");
    if (!subquery)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'subquery' during AST JSON deserialization");
    children.push_back(subquery);

    /// `aliases` is an `ASTExpressionList` of `ASTIdentifier`; `QueryTreeBuilder::buildSelectExpression`
    /// does `aliases->as<ASTExpressionList &>()` then `column_alias->as<ASTIdentifier &>()`.
    aliases = r.readChildOfType<ASTExpressionList>("aliases");
    if (aliases)
    {
        for (const auto & alias : aliases->children)
            if (!alias || !alias->as<ASTIdentifier>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`WithElement` aliases must be identifiers during AST JSON deserialization");
        children.push_back(aliases);
    }
}

void ASTWithElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    if (aliases)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << "(";
        aliases->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
    ostr << " AS" << (is_materialized ? " MATERIALIZED" : "");
    ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(ostr, settings, state, frame);
}

}
