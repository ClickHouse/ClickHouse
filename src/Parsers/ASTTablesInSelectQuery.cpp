#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

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
    auto res = std::make_shared<ASTTableExpression>(*this);
    res->children.clear();

    CLONE(database_and_table_name);
    CLONE(table_function);
    CLONE(subquery);
    CLONE(sample_size);
    CLONE(sample_offset);

    return res;
}

void ASTTableJoin::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(locality);
    hash_state.update(strictness);
    hash_state.update(kind);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTTableJoin::clone() const
{
    auto res = std::make_shared<ASTTableJoin>(*this);
    res->children.clear();

    CLONE(using_expression_list);
    CLONE(on_expression);

    return res;
}

void ASTTableJoin::forEachPointerToChild(std::function<void(void **)> f)
{
    IAST * new_using_expression_list = using_expression_list.get();
    f(reinterpret_cast<void **>(&new_using_expression_list));
    if (new_using_expression_list != using_expression_list.get())
    {
        if (new_using_expression_list)
            using_expression_list = new_using_expression_list->ptr();
        else
            using_expression_list.reset();
    }

    IAST * new_on_expression = on_expression.get();
    f(reinterpret_cast<void **>(&new_on_expression));
    if (new_on_expression != on_expression.get())
    {
        if (new_on_expression)
            on_expression = new_on_expression->ptr();
        else
            on_expression.reset();
    }
}

void ASTArrayJoin::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(kind);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTArrayJoin::clone() const
{
    auto res = std::make_shared<ASTArrayJoin>(*this);
    res->children.clear();

    CLONE(expression_list);

    return res;
}

ASTPtr ASTTablesInSelectQueryElement::clone() const
{
    auto res = std::make_shared<ASTTablesInSelectQueryElement>(*this);
    res->children.clear();

    CLONE(table_join);
    CLONE(table_expression);
    CLONE(array_join);

    return res;
}

ASTPtr ASTTablesInSelectQuery::clone() const
{
    const auto res = std::make_shared<ASTTablesInSelectQuery>(*this);
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
    else if (table_function && !(table_function->as<ASTFunction>()->prefer_subquery_to_function_formatting && subquery))
    {
        ostr << " ";
        table_function->format(ostr, settings, state, frame);
    }
    else if (subquery)
    {
        ostr << settings.nl_or_ws << indent_str;
        subquery->format(ostr, settings, state, frame);
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
        /// We should always print alias for 'USING (a AS b)' syntax (supported with analyzer only).
        /// Otherwise query like 'SELECT a AS b FROM t1 JOIN t2 USING (a AS b)' will be broken.
        /// See 03448_analyzer_array_join_alias_in_join_using_bug.sql
        frame.ignore_printed_asts_with_alias = true;
        using_expression_list->format(ostr, settings, state, frame);
        ostr << ")";
    }
    else if (on_expression)
    {
        ostr << " ON ";
        /// If there is an alias for the whole expression parens should be added, otherwise it will be invalid syntax
        bool on_has_alias = !on_expression->tryGetAlias().empty();
        if (on_has_alias)
            ostr << "(";
        on_expression->format(ostr, settings, state, frame);
        if (on_has_alias)
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

}
