#include <Parsers/ASTProjectionDeclaration.h>

#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTProjectionDeclaration::clone() const
{
    auto res = make_intrusive<ASTProjectionDeclaration>();
    res->name = name;
    if (query)
        res->set(res->query, query->clone());
    if (index)
        res->set(res->index, index->clone());
    if (type)
        res->set(res->type, type->clone());
    if (with_settings)
        res->set(res->with_settings, with_settings->clone());
    if (columns)
        res->set(res->columns, columns->clone());
    return res;
}

void ASTProjectionDeclaration::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `name` is not a child, so the default implementation does not see it.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(
        sizeof(void *) != 8 || sizeof(*this) == 96,
        "If members were added to ASTProjectionDeclaration, hash them here unless they are purely cosmetic.");
    hash_state.update(name.size());
    hash_state.update(name);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


void ASTProjectionDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ProjectionDeclaration");
    w.writeString("name", name);
    w.writeChild("query", query);
    w.writeChild("index", index);
    w.writeChild("projection_type", type);
    w.writeChild("with_settings", with_settings);
    w.writeChild("columns", columns);
}

void ASTProjectionDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    /// `query` is the parser-owned `ASTProjectionSelectQuery`; `ProjectionDescription::getProjectionFromAST`
    /// does `query->as<ASTProjectionSelectQuery &>()`, so reject any other node type here.
    auto query_child = r.readChildOfType<ASTProjectionSelectQuery>("query");
    if (query_child)
        set(query, query_child);

    /// `index` is produced by the parser only as a non-empty `ASTExpressionList`
    /// (`ParserProjectionDeclaration` uses `ParserNotEmptyExpressionList`). With `TYPE commit_order`,
    /// `ProjectionIndexCommitOrder::fillProjectionDescription` clones `index` straight into the
    /// projection SELECT slot, and `ASTProjectionSelectQuery::cloneToASTSelect` throws a logical
    /// error unless that slot is an `ASTExpressionList` — so reject any other shape at the boundary.
    auto index_child = r.readChildOfType<ASTExpressionList>("index");
    if (index_child)
    {
        if (index_child->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`ProjectionDeclaration` INDEX must be a non-empty expression list during AST JSON deserialization");
        set(index, index_child);
    }

    /// `projection_type` and `with_settings` are typed members (`ASTFunction *` / `ASTSetQuery *`);
    /// restoring them through the generic child path would let a wrong node type reach `IAST::set`
    /// as an internal cast error instead of a user-facing `BAD_ARGUMENTS`.
    auto type_child = r.readChildOfType<ASTFunction>("projection_type");
    if (type_child)
        set(type, type_child);

    auto with_settings_child = r.readChildOfType<ASTSetQuery>("with_settings");
    if (with_settings_child)
        set(with_settings, with_settings_child);

    /// `ProjectionDescription` reads each element with `as<ASTColumnDeclaration &>`, so reject any other
    /// shape here rather than let it surface as an internal cast error.
    auto columns_child = r.readChildOfType<ASTExpressionList>("columns");
    if (columns_child)
    {
        if (columns_child->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`ProjectionDeclaration` column list must be non-empty during AST JSON deserialization");
        for (const auto & child : columns_child->children)
        {
            if (!child->as<ASTColumnDeclaration>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`ProjectionDeclaration` column list must contain only column declarations "
                    "during AST JSON deserialization");
        }
        set(columns, columns_child);
    }

    /// A `ProjectionDeclaration` has exactly two parser-produced shapes: a `(SELECT ...)` projection
    /// (`query` set) or an `INDEX ... TYPE ...` projection (`index` and `type` set together). Reject
    /// parser-impossible combinations so `formatImpl` cannot print SQL the parser would never produce
    /// (e.g. `p INDEX a` without `TYPE`, or both a query and an index).
    if (query && (index || type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ProjectionDeclaration` cannot have both a SELECT query and an INDEX during AST JSON deserialization");
    if (static_cast<bool>(index) != static_cast<bool>(type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ProjectionDeclaration` INDEX requires TYPE during AST JSON deserialization");
    if (!query && !index)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ProjectionDeclaration` must have either a SELECT query or an INDEX during AST JSON deserialization");

    if (columns && !query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ProjectionDeclaration` column list requires a `SELECT` query during AST JSON deserialization");
}

void ASTProjectionDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    formatBody(ostr, settings, state, frame);
}

void ASTProjectionDeclaration::formatBody(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// A column list is only ever printed with the query it belongs to: on its own it would format
    /// to `p (...) AS` with nothing after `AS`, which does not parse back.
    if (columns && query)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        ostr << settings.nl_or_ws << indent_str << "(";
        if (settings.one_line)
        {
            /// `ASTAlterQuery` sets `expression_list_prepend_whitespace`, which would emit a stray space.
            FormatStateStacked frame_nested = frame;
            frame_nested.expression_list_prepend_whitespace = false;
            frame_nested.surround_each_list_element_with_parens = false;
            columns->format(ostr, settings, state, frame_nested);
        }
        else
        {
            /// `formatImplMultiline` writes its own newline and indentation, so `frame` passes through
            /// unchanged; the flag keeps a single declared column on its own line.
            FormatStateStacked frame_nested = frame;
            frame_nested.expression_list_always_start_on_new_line = true;
            columns->as<ASTExpressionList &>().formatImplMultiline(ostr, settings, state, frame_nested);
        }
        ostr << nl_or_nothing << indent_str << ")";
        ostr << settings.nl_or_ws << indent_str << "AS";
    }

    if (query)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        ostr << settings.nl_or_ws << indent_str << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        ++frame_nested.indent;
        query->format(ostr, settings, state, frame_nested);
        ostr << nl_or_nothing << indent_str << ")";
    }

    if (index)
    {
        ostr << " INDEX ";
        index->format(ostr, settings, state, frame);
    }

    if (type)
    {
        ostr << " TYPE ";
        type->format(ostr, settings, state, frame);
    }

    if (with_settings)
    {
        ostr << " WITH SETTINGS (";
        with_settings->format(ostr, settings, state, frame);
        ostr << ")";
    }
}
}
