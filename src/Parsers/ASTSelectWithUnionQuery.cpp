#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/SelectUnionMode.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTSelectWithUnionQuery::clone() const
{
    auto res = make_intrusive<ASTSelectWithUnionQuery>(*this);
    res->children.clear();

    res->list_of_selects = list_of_selects->clone();
    res->children.push_back(res->list_of_selects);

    res->union_mode = union_mode;
    res->is_normalized = is_normalized;
    res->list_of_modes = list_of_modes;
    res->set_of_modes = set_of_modes;

    cloneOutputOptions(*res);
    return res;
}


void ASTSelectWithUnionQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    auto mode_to_str = [&](SelectUnionMode mode)
    {
        if (mode == SelectUnionMode::UNION_DEFAULT)
            return "UNION";
        if (mode == SelectUnionMode::UNION_ALL)
            return "UNION ALL";
        if (mode == SelectUnionMode::UNION_DISTINCT)
            return "UNION DISTINCT";
        if (mode == SelectUnionMode::EXCEPT_DEFAULT)
            return "EXCEPT";
        if (mode == SelectUnionMode::EXCEPT_ALL)
            return "EXCEPT ALL";
        if (mode == SelectUnionMode::EXCEPT_DISTINCT)
            return "EXCEPT DISTINCT";
        if (mode == SelectUnionMode::INTERSECT_DEFAULT)
            return "INTERSECT";
        if (mode == SelectUnionMode::INTERSECT_ALL)
            return "INTERSECT ALL";
        if (mode == SelectUnionMode::INTERSECT_DISTINCT)
            return "INTERSECT DISTINCT";
        return "";
    };

    auto is_except = [](SelectUnionMode mode)
    {
        return mode == SelectUnionMode::EXCEPT_DEFAULT
            || mode == SelectUnionMode::EXCEPT_ALL
            || mode == SelectUnionMode::EXCEPT_DISTINCT;
    };

    auto get_mode = [&](ASTs::const_iterator it) -> SelectUnionMode
    {
        if (is_normalized)
            return union_mode;
        auto index = static_cast<size_t>(it - list_of_selects->children.begin()) - 1;
        if (index >= list_of_modes.size())
            return union_mode;
        return list_of_modes[index];
    };

    for (ASTs::const_iterator it = list_of_selects->children.begin(); it != list_of_selects->children.end(); ++it)
    {
        if (it != list_of_selects->children.begin())
        {
            ostr << settings.nl_or_ws << indent_str
                << mode_to_str(get_mode(it))

                << settings.nl_or_ws;
        }

        bool need_parens = false;

        /// EXCEPT can be confused with the asterisk modifier:
        /// SELECT * EXCEPT SELECT 1 -- two queries
        /// SELECT * EXCEPT col      -- a modifier for asterisk
        /// For this reason, add parentheses when formatting any side of EXCEPT.
        ASTs::const_iterator next = it;
        ++next;
        if ((it != list_of_selects->children.begin() && is_except(get_mode(it)))
            || (next != list_of_selects->children.end() && is_except(get_mode(next))))
            need_parens = true;

        /// If this is a subtree with another chain of selects, we also need parens.
        auto * union_node = (*it)->as<ASTSelectWithUnionQuery>();
        if (union_node)
            need_parens = true;

        /// When `settings_ast` is set on the whole SelectWithUnionQuery (inherited
        /// from ASTQueryWithOutput), or a parent query (e.g. EXPLAIN) will append
        /// SETTINGS after this node (signalled via `frame.parent_has_trailing_settings`),
        /// and no `out_file` or `format_ast` precedes it in the formatted output,
        /// the base class formats `SETTINGS ...` immediately after the UNION chain.
        /// Without parentheses around individual SELECTs, the re-parser's
        /// `ParserSelectQuery` would consume SETTINGS as part of the last individual
        /// SELECT, moving it from the outer query to the last SelectQuery and breaking
        /// the formatting roundtrip.
        /// When `out_file` or `format_ast` is present, they are formatted before
        /// SETTINGS, and `ParserSelectQuery` stops before them (it doesn't handle
        /// INTO OUTFILE or FORMAT), so SETTINGS remains on the outer query.
        /// Wrapping each SELECT in parentheses prevents this: the parser treats
        /// each `(SELECT ...)` as a self-contained subquery, and SETTINGS stays on
        /// the outer query. `ParserUnionQueryElement` flattens single-child
        /// subqueries back to `SelectQuery`, preserving the AST structure.
        if ((settings_ast || frame.parent_has_trailing_settings) && !out_file && !format_ast && (*it)->as<ASTSelectQuery>())
            need_parens = true;

        if (need_parens)
        {
            ostr << indent_str;
            auto subquery = make_intrusive<ASTSubquery>(*it);
            subquery->format(ostr, settings, state, frame);
        }
        else
        {
            (*it)->format(ostr, settings, state, frame);
        }
    }
}


bool ASTSelectWithUnionQuery::hasNonDefaultUnionMode() const
{
    return set_of_modes.contains(SelectUnionMode::UNION_DISTINCT) || set_of_modes.contains(SelectUnionMode::INTERSECT_DISTINCT)
        || set_of_modes.contains(SelectUnionMode::EXCEPT_DISTINCT);
}

bool ASTSelectWithUnionQuery::hasQueryParameters() const
{
    if (!has_query_parameters.has_value())
    {
        for (const auto & child : list_of_selects->children)
        {
            if (auto * select_node = child->as<ASTSelectQuery>())
            {
                if (select_node->hasQueryParameters())
                {
                    has_query_parameters = true;
                    return has_query_parameters.value();
                }
            }
        }
        has_query_parameters = false;
    }

    return  has_query_parameters.value();
}

NameToNameMap ASTSelectWithUnionQuery::getQueryParameters() const
{
    NameToNameMap query_params;

    if (!hasQueryParameters())
        return {};

    for (const auto & child : list_of_selects->children)
    {
        if (auto * select_node = child->as<ASTSelectQuery>())
        {
            if (select_node->hasQueryParameters())
            {
                NameToNameMap select_node_param = select_node->getQueryParameters();
                query_params.insert(select_node_param.begin(), select_node_param.end());
            }
        }
    }

    return query_params;
}

void ASTSelectWithUnionQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "SelectWithUnionQuery");

    w.writeString("union_mode", toString(union_mode));

    if (!list_of_modes.empty())
    {
        w.writeKey("list_of_modes");
        auto & o = w.getOut();
        o << '[';
        for (size_t i = 0; i < list_of_modes.size(); ++i)
        {
            if (i > 0) o << ',';
            writeJSONString(toString(list_of_modes[i]), o, w.getFormatSettings());
        }
        o << ']';
    }

    w.writeChild("list_of_selects", list_of_selects);
    w.writeChild("out_file", out_file);
    w.writeChild("format_ast", format_ast);
    w.writeChild("settings_ast", settings_ast);
    w.writeChild("compression", compression);
    w.writeChild("compression_level", compression_level);

    /// Output-option flags from `ASTQueryWithOutput`: without these, `INTO OUTFILE ... APPEND`,
    /// `INTO OUTFILE ... TRUNCATE`, and `INTO OUTFILE ... AND STDOUT` would be silently lost on round-trip.
    w.writeBool("is_outfile_append", isOutfileAppend());
    w.writeBool("is_outfile_truncate", isOutfileTruncate());
    w.writeBool("is_into_outfile_with_stdout", isIntoOutfileWithStdout());
}

void ASTSelectWithUnionQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    union_mode = parseSelectUnionMode(r.getString("union_mode", "UNION_DEFAULT"));

    auto modes_arr = r.readStringArray("list_of_modes");
    for (const auto & mode_str : modes_arr)
        list_of_modes.push_back(parseSelectUnionMode(mode_str));

    /// `list_of_selects` is a required invariant: `clone`, `formatQueryImpl`, and the interpreters
    /// dereference it unconditionally. Reject malformed JSON that omits it instead of producing an
    /// AST that crashes later. It must be a non-empty `ASTExpressionList`, because `formatQueryImpl`
    /// iterates its children and dereferences each element.
    list_of_selects = r.readChild("list_of_selects");
    if (!list_of_selects)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SelectWithUnionQuery` AST requires 'list_of_selects' during AST JSON deserialization");
    if (!list_of_selects->as<ASTExpressionList>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SelectWithUnionQuery` AST requires 'list_of_selects' to be an `ASTExpressionList` during AST JSON deserialization");
    if (list_of_selects->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SelectWithUnionQuery` AST requires a non-empty 'list_of_selects' during AST JSON deserialization");
    children.push_back(list_of_selects);

    /// `list_of_modes` describes the separators between adjacent selects, so its cardinality must be
    /// exactly one less than the number of selects. `formatQueryImpl` indexes `list_of_modes` by
    /// `(position - 1)`, so a mismatch would either read stale modes or leave gaps; reject it.
    if (!list_of_modes.empty() && list_of_modes.size() != list_of_selects->children.size() - 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SelectWithUnionQuery` AST has {} entries in 'list_of_modes' but expected {} for {} selects "
            "during AST JSON deserialization",
            list_of_modes.size(), list_of_selects->children.size() - 1, list_of_selects->children.size());

    out_file = r.readChild("out_file");
    if (out_file)
        children.push_back(out_file);

    format_ast = r.readChild("format_ast");
    if (format_ast)
        children.push_back(format_ast);

    settings_ast = r.readChild("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);

    compression = r.readChild("compression");
    if (compression)
        children.push_back(compression);

    compression_level = r.readChild("compression_level");
    if (compression_level)
        children.push_back(compression_level);

    /// Restore output-option flags from `ASTQueryWithOutput` (see `writeJSON`).
    setIsOutfileAppend(r.getBool("is_outfile_append"));
    setIsOutfileTruncate(r.getBool("is_outfile_truncate"));
    setIsIntoOutfileWithStdout(r.getBool("is_into_outfile_with_stdout"));
}

}
