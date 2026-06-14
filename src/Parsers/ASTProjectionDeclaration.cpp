#include <Parsers/ASTProjectionDeclaration.h>

#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFunction.h>
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
    return res;
}


void ASTProjectionDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ProjectionDeclaration");
    w.writeString("name", name);
    w.writeChild("query", query);
    w.writeChild("index", index);
    w.writeChild("projection_type", type);
    w.writeChild("with_settings", with_settings);
}

void ASTProjectionDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    auto query_child = r.readChild("query");
    if (query_child)
        set(query, query_child);

    auto index_child = r.readChild("index");
    if (index_child)
        set(index, index_child);

    /// `projection_type` and `with_settings` are typed members (`ASTFunction *` / `ASTSetQuery *`);
    /// restoring them through the generic child path would let a wrong node type reach `IAST::set`
    /// as an internal cast error instead of a user-facing `BAD_ARGUMENTS`.
    auto type_child = r.readChildOfType<ASTFunction>("projection_type");
    if (type_child)
        set(type, type_child);

    auto with_settings_child = r.readChildOfType<ASTSetQuery>("with_settings");
    if (with_settings_child)
        set(with_settings, with_settings_child);

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
}

void ASTProjectionDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
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
