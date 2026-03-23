#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define CLONE(member) \
do \
{ \
    if (member) \
    { \
        res->member = (member)->clone(); \
        res->children.push_back(res->member); \
    } \
} \
while (false)


void ASTTableExpression::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(final);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


ASTPtr ASTTableExpression::clone() const
{
    auto res = make_intrusive<ASTTableExpression>(*this);
    res->children.clear();

    CLONE(database_and_table_name);
    CLONE(table_function);
    CLONE(subquery);
    CLONE(sample_size);
    CLONE(sample_offset);
    CLONE(column_aliases);

    return res;
}

void ASTTableJoin::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(locality);
    hash_state.update(strictness);
    hash_state.update(kind);
    hash_state.update(is_natural);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTTableJoin::clone() const
{
    auto res = make_intrusive<ASTTableJoin>(*this);
    res->children.clear();

    CLONE(using_expression_list);
    CLONE(on_expression);

    return res;
}

void ASTTableJoin::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    f(nullptr, &using_expression_list);
    f(nullptr, &on_expression);
}

void ASTArrayJoin::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(kind);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTArrayJoin::clone() const
{
    auto res = make_intrusive<ASTArrayJoin>(*this);
    res->children.clear();

    CLONE(expression_list);

    return res;
}

ASTPtr ASTTablesInSelectQueryElement::clone() const
{
    auto res = make_intrusive<ASTTablesInSelectQueryElement>(*this);
    res->children.clear();

    CLONE(table_join);
    CLONE(table_expression);
    CLONE(array_join);

    return res;
}

ASTPtr ASTTablesInSelectQuery::clone() const
{
    const auto res = make_intrusive<ASTTablesInSelectQuery>(*this);
    res->children.clear();

    for (const auto & child : children)
        res->children.emplace_back(child->clone());

    return res;
}

#undef CLONE


void ASTTableExpression::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (database_and_table_name)
    {
        ostr << " ";
        database_and_table_name->format(ostr, settings, state, frame);
    }
    else if (table_function && !(table_function->as<ASTFunction>()->preferSubqueryToFunctionFormatting() && subquery))
    {
        ostr << " ";
        table_function->format(ostr, settings, state, frame);
    }
    else if (subquery)
    {
        ostr << settings.nl_or_ws << indent_str;
        subquery->format(ostr, settings, state, frame);
    }

    /// format column aliases (`AS t(a, b)` -> the (a, b) part)
    if (column_aliases)
    {
        ostr << "(";
        auto column_aliases_frame = frame;
        column_aliases_frame.expression_list_prepend_whitespace = false;
        column_aliases->format(ostr, settings, state, column_aliases_frame);
        ostr << ")";
    }

    if (final)
    {
        ostr << settings.nl_or_ws << indent_str
            << "FINAL";
    }

    if (sample_size)
    {
        ostr << settings.nl_or_ws << indent_str
            << "SAMPLE ";
        sample_size->format(ostr, settings, state, frame);

        if (sample_offset)
        {
            ostr << ' '
                << "OFFSET ";
            sample_offset->format(ostr, settings, state, frame);
        }
    }
}


void ASTTableJoin::formatImplBeforeTable(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (kind != JoinKind::Comma)
        ostr << settings.nl_or_ws << indent_str;

    switch (locality)
    {
        case JoinLocality::Unspecified:
        case JoinLocality::Local:
            break;
        case JoinLocality::Global:
            ostr << "GLOBAL ";
            break;
    }

    if (kind != JoinKind::Cross && kind != JoinKind::Comma)
    {
        switch (strictness)
        {
            case JoinStrictness::Unspecified:
                break;
            case JoinStrictness::RightAny:
            case JoinStrictness::Any:
                ostr << "ANY ";
                break;
            case JoinStrictness::All:
                ostr << "ALL ";
                break;
            case JoinStrictness::Asof:
                ostr << "ASOF ";
                break;
            case JoinStrictness::Semi:
                ostr << "SEMI ";
                break;
            case JoinStrictness::Anti:
                ostr << "ANTI ";
                break;
        }
    }

    if (is_natural)
        ostr << "NATURAL ";

    switch (kind)
    {
        case JoinKind::Inner:
            ostr << "INNER JOIN";
            break;
        case JoinKind::Left:
            ostr << "LEFT JOIN";
            break;
        case JoinKind::Right:
            ostr << "RIGHT JOIN";
            break;
        case JoinKind::Full:
            ostr << "FULL OUTER JOIN";
            break;
        case JoinKind::Cross:
            ostr << "CROSS JOIN";
            break;
        case JoinKind::Comma:
            ostr << ",";
            break;
        case JoinKind::Paste:
            ostr << "PASTE JOIN";
            break;
    }
}


void ASTTableJoin::formatImplAfterTable(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    frame.expression_list_prepend_whitespace = false;

    if (using_expression_list)
    {
        ostr << " USING ";
        ostr << "(";
        using_expression_list->format(ostr, settings, state, frame);
        ostr << ")";
    }
    else if (on_expression)
    {
        ostr << " ON ";

       /** If there is an alias for the whole expression we wrap the ON clause in parens in two cases:
         *  1. collapse_identical_nodes_to_aliases is true (meaning old analyzer is being used) AND the alias was
         *     defined earlier in the query
         *  2. collapse_identical_nodes_to_aliases is false (the analyzer) - because we will not make any substitutions
         */
        bool on_need_parens = false;
        auto on_alias = on_expression->tryGetAlias();
        if (!on_alias.empty())
        {
            bool was_alias_defined_earlier = state.printed_asts_with_alias.contains(
                {frame.current_select, on_alias, on_expression->getTreeHash(/*ignore_aliases=*/true)});
            on_need_parens = settings.collapse_identical_nodes_to_aliases ? !was_alias_defined_earlier : true;
        }


        if (on_need_parens)
            ostr << "(";
        on_expression->format(ostr, settings, state, frame);
        if (on_need_parens)
            ostr << ")";
    }
}


void ASTTableJoin::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    formatImplBeforeTable(ostr, settings, state, frame);
    ostr << " ...";
    formatImplAfterTable(ostr, settings, state, frame);
}


void ASTArrayJoin::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
    frame.expression_list_prepend_whitespace = true;

    ostr
        << settings.nl_or_ws
        << indent_str
        << (kind == Kind::Left ? "LEFT " : "") << "ARRAY JOIN";

    settings.one_line
        ? expression_list->format(ostr, settings, state, frame)
        : expression_list->as<ASTExpressionList &>().formatImplMultiline(ostr, settings, state, frame);
}


void ASTTablesInSelectQueryElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (table_expression)
    {
        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplBeforeTable(ostr, settings, state, frame);

        table_expression->format(ostr, settings, state, frame);

        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplAfterTable(ostr, settings, state, frame);
    }
    else if (array_join)
    {
        array_join->format(ostr, settings, state, frame);
    }
}


void ASTTablesInSelectQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & child : children)
        child->format(ostr, settings, state, frame);
}


void ASTTablesInSelectQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TablesInSelectQuery");
    w.writeChildren(children);
}

void ASTTablesInSelectQueryElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TablesInSelectQueryElement");
    w.writeChild("table_join", table_join);
    w.writeChild("table_expression", table_expression);
    w.writeChild("array_join", array_join);
}

void ASTTableExpression::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TableExpression");
    if (final)
        w.writeBool("final", true);
    w.writeChild("database_and_table_name", database_and_table_name);
    w.writeChild("table_function", table_function);
    w.writeChild("subquery", subquery);
    w.writeChild("sample_size", sample_size);
    w.writeChild("sample_offset", sample_offset);
    w.writeChild("column_aliases", column_aliases);
}

void ASTTableJoin::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TableJoin");
    if (locality != JoinLocality::Unspecified)
        w.writeString("locality", toString(locality));
    if (strictness != JoinStrictness::Unspecified)
        w.writeString("strictness", toString(strictness));
    w.writeString("kind", toString(kind));
    if (is_natural)
        w.writeBool("is_natural", true);
    w.writeChild("using_expression_list", using_expression_list);
    w.writeChild("on_expression", on_expression);
}

void ASTArrayJoin::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ArrayJoin");
    w.writeString("kind", kind == Kind::Left ? "Left" : "Inner");
    w.writeChild("expression_list", expression_list);
}

static JoinKind parseJoinKind(const String & s)
{
    if (s == "INNER") return JoinKind::Inner;
    if (s == "LEFT") return JoinKind::Left;
    if (s == "RIGHT") return JoinKind::Right;
    if (s == "FULL") return JoinKind::Full;
    if (s == "CROSS") return JoinKind::Cross;
    if (s == "COMMA") return JoinKind::Comma;
    if (s == "PASTE") return JoinKind::Paste;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown JoinKind: '{}'", s);
}

static JoinStrictness parseJoinStrictness(const String & s)
{
    if (s == "RIGHT_ANY") return JoinStrictness::RightAny;
    if (s == "ANY") return JoinStrictness::Any;
    if (s == "ALL") return JoinStrictness::All;
    if (s == "ASOF") return JoinStrictness::Asof;
    if (s == "SEMI") return JoinStrictness::Semi;
    if (s == "ANTI") return JoinStrictness::Anti;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown JoinStrictness: '{}'", s);
}

static JoinLocality parseJoinLocality(const String & s)
{
    if (s == "LOCAL") return JoinLocality::Local;
    if (s == "GLOBAL") return JoinLocality::Global;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown JoinLocality: '{}'", s);
}

void ASTTablesInSelectQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    children = r.readChildren();
}

void ASTTablesInSelectQueryElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto child = r.readChild("table_join");
    if (child)
    {
        table_join = child;
        children.push_back(table_join);
    }

    child = r.readChild("table_expression");
    if (child)
    {
        table_expression = child;
        children.push_back(table_expression);
    }

    child = r.readChild("array_join");
    if (child)
    {
        array_join = child;
        children.push_back(array_join);
    }
}

void ASTTableExpression::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    final = r.getBool("final");

    auto child = r.readChild("database_and_table_name");
    if (child)
    {
        database_and_table_name = child;
        children.push_back(database_and_table_name);
    }

    child = r.readChild("table_function");
    if (child)
    {
        table_function = child;
        children.push_back(table_function);
    }

    child = r.readChild("subquery");
    if (child)
    {
        subquery = child;
        children.push_back(subquery);
    }

    child = r.readChild("sample_size");
    if (child)
    {
        sample_size = child;
        children.push_back(sample_size);
    }

    child = r.readChild("sample_offset");
    if (child)
    {
        sample_offset = child;
        children.push_back(sample_offset);
    }

    child = r.readChild("column_aliases");
    if (child)
    {
        column_aliases = child;
        children.push_back(column_aliases);
    }
}

void ASTTableJoin::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String locality_str = r.getString("locality");
    if (!locality_str.empty())
        locality = parseJoinLocality(locality_str);

    String strictness_str = r.getString("strictness");
    if (!strictness_str.empty())
        strictness = parseJoinStrictness(strictness_str);

    kind = parseJoinKind(r.getString("kind"));
    is_natural = r.getBool("is_natural");

    auto child = r.readChild("using_expression_list");
    if (child)
    {
        using_expression_list = child;
        children.push_back(using_expression_list);
    }

    child = r.readChild("on_expression");
    if (child)
    {
        on_expression = child;
        children.push_back(on_expression);
    }
}

void ASTArrayJoin::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    String kind_str = r.getString("kind");
    kind = (kind_str == "Left") ? Kind::Left : Kind::Inner;

    auto child = r.readChild("expression_list");
    if (child)
    {
        expression_list = child;
        children.push_back(expression_list);
    }
}

}
