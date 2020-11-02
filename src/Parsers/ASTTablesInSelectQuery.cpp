#include <Common/typeid_cast.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/SipHash.h>


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


void ASTTableExpression::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(final);
    IAST::updateTreeHashImpl(hash_state);
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

void ASTTableJoin::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(locality);
    hash_state.update(strictness);
    hash_state.update(kind);
    IAST::updateTreeHashImpl(hash_state);
}

ASTPtr ASTTableJoin::clone() const
{
    auto res = std::make_shared<ASTTableJoin>(*this);
    res->children.clear();

    CLONE(using_expression_list);
    CLONE(on_expression);

    return res;
}

void ASTArrayJoin::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(kind);
    IAST::updateTreeHashImpl(hash_state);
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


void ASTTableExpression::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (database_and_table_name)
    {
        database_and_table_name->formatImpl(settings, state, frame);
    }
    else if (table_function)
    {
        table_function->formatImpl(settings, state, frame);
    }
    else if (subquery)
    {
        subquery->formatImpl(settings, state, frame);
    }

    if (final)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str
            << "FINAL" << (settings.hilite ? hilite_none : "");
    }

    if (sample_size)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str
            << "SAMPLE " << (settings.hilite ? hilite_none : "");
        sample_size->formatImpl(settings, state, frame);

        if (sample_offset)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << ' '
                << "OFFSET " << (settings.hilite ? hilite_none : "");
            sample_offset->formatImpl(settings, state, frame);
        }
    }
}


void ASTTableJoin::formatImplBeforeTable(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    switch (locality)
    {
        case Locality::Unspecified:
            break;
        case Locality::Local:
            break;
        case Locality::Global:
            settings.ostr << "GLOBAL ";
            break;
    }

    if (kind != Kind::Cross && kind != Kind::Comma)
    {
        switch (strictness)
        {
            case Strictness::Unspecified:
                break;
            case Strictness::RightAny:
            case Strictness::Any:
                settings.ostr << "ANY ";
                break;
            case Strictness::All:
                settings.ostr << "ALL ";
                break;
            case Strictness::Asof:
                settings.ostr << "ASOF ";
                break;
            case Strictness::Semi:
                settings.ostr << "SEMI ";
                break;
            case Strictness::Anti:
                settings.ostr << "ANTI ";
                break;
        }
    }

    switch (kind)
    {
        case Kind::Inner:
            settings.ostr << "INNER JOIN";
            break;
        case Kind::Left:
            settings.ostr << "LEFT JOIN";
            break;
        case Kind::Right:
            settings.ostr << "RIGHT JOIN";
            break;
        case Kind::Full:
            settings.ostr << "FULL OUTER JOIN";
            break;
        case Kind::Cross:
            settings.ostr << "CROSS JOIN";
            break;
        case Kind::Comma:
            settings.ostr << ",";
            break;
    }

    settings.ostr << (settings.hilite ? hilite_none : "");
}


void ASTTableJoin::formatImplAfterTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    frame.expression_list_prepend_whitespace = false;

    if (using_expression_list)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " USING " << (settings.hilite ? hilite_none : "");
        settings.ostr << "(";
        using_expression_list->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }
    else if (on_expression)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        on_expression->formatImpl(settings, state, frame);
    }
}


void ASTTableJoin::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    formatImplBeforeTable(settings, state, frame);
    settings.ostr << " ... ";
    formatImplAfterTable(settings, state, frame);
}


void ASTArrayJoin::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.expression_list_prepend_whitespace = true;

    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << (kind == Kind::Left ? "LEFT " : "") << "ARRAY JOIN" << (settings.hilite ? hilite_none : "");

    settings.one_line
        ? expression_list->formatImpl(settings, state, frame)
        : expression_list->as<ASTExpressionList &>().formatImplMultiline(settings, state, frame);
}


void ASTTablesInSelectQueryElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (table_expression)
    {
        if (table_join)
        {
            table_join->as<ASTTableJoin &>().formatImplBeforeTable(settings, state, frame);
            settings.ostr << " ";
        }

        table_expression->formatImpl(settings, state, frame);

        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplAfterTable(settings, state, frame);
    }
    else if (array_join)
    {
        array_join->formatImpl(settings, state, frame);
    }
}


void ASTTablesInSelectQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            settings.ostr << settings.nl_or_ws << indent_str;

        (*it)->formatImpl(settings, state, frame);
    }
}

}
