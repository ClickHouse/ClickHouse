#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/SipHash.h>
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
    if (key_columns)
        res->key_columns = key_columns->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// The name selects which CTE a reference resolves to, `aliases` renames the subquery
    /// columns and `key_columns` selects keyed recursive evaluation, but none of them is a
    /// child, so without this they are all absent from the hash.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 88, "If members were added to ASTWithElement, hash them here unless they are purely cosmetic.");
    /// Length-prefixed, otherwise the name runs into whatever `getID` writes next.
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(is_materialized);
    hash_state.update(aliases != nullptr);
    if (aliases)
        aliases->updateTreeHash(hash_state, ignore_aliases);
    hash_state.update(key_columns != nullptr);
    if (key_columns)
        key_columns->updateTreeHash(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTWithElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "WithElement");
    w.writeString("name", name);
    if (is_materialized)
        w.writeBool("is_materialized", true);
    w.writeChild("subquery", subquery);
    w.writeChild("aliases", aliases);
    w.writeChild("key_columns", key_columns);
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
        /// `ParserWithElement` and `clone` keep `aliases` out of `children`, and
        /// `updateTreeHashImpl` hashes the member explicitly, so adding it here would make a
        /// JSON-built copy of the same definition hash the aliases twice and compare unequal.
    }

    /// `key_columns` is an `ASTExpressionList` of `ASTIdentifier` as well: `QueryTreeBuilder`
    /// does `key_column->as<ASTIdentifier &>()` over its children. It is kept out of `children`
    /// for the same reason as `aliases`.
    key_columns = r.readChildOfType<ASTExpressionList>("key_columns");
    if (key_columns)
    {
        for (const auto & key_column : key_columns->children)
            if (!key_column || !key_column->as<ASTIdentifier>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`WithElement` USING KEY columns must be identifiers during AST JSON deserialization");
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
    if (key_columns)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << " USING KEY (";
        key_columns->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
    ostr << " AS" << (is_materialized ? " MATERIALIZED" : "");
    ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(ostr, settings, state, frame);
}

}
