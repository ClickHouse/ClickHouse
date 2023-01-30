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
        settings.ostr << " ";
        database_and_table_name->formatImpl(settings, state, frame);
    }
    else if (table_function && !(table_function->as<ASTFunction>()->prefer_subquery_to_function_formatting && subquery))
    {
        settings.ostr << " ";
        table_function->formatImpl(settings, state, frame);
    }
    else if (subquery)
    {
        settings.ostr << settings.nl_or_ws << indent_str;
        subquery->formatImpl(settings, state, frame);
    }

    if (final)
    {
        settings.ostr << settings.nl_or_ws << indent_str;
        settings.writeKeyword("FINAL");
    }

    if (sample_size)
    {
        settings.ostr << settings.nl_or_ws << indent_str;
        settings.writeKeyword("SAMPLE ");
        sample_size->formatImpl(settings, state, frame);

        if (sample_offset)
        {
            settings.writeKeyword(" OFFSET ");
            sample_offset->formatImpl(settings, state, frame);
        }
    }
}


void ASTTableJoin::formatImplBeforeTable(const FormatSettings & settings, FormatState &, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (kind != JoinKind::Comma)
    {
        settings.ostr << settings.nl_or_ws << indent_str;
    }

    switch (locality)
    {
        case JoinLocality::Unspecified:
        case JoinLocality::Local:
            break;
        case JoinLocality::Global:
            settings.writeKeyword("GLOBAL ");
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
                settings.writeKeyword("ANY ");
                break;
            case JoinStrictness::All:
                settings.writeKeyword("ALL ");
                break;
            case JoinStrictness::Asof:
                settings.writeKeyword("ASOF ");
                break;
            case JoinStrictness::Semi:
                settings.writeKeyword("SEMI ");
                break;
            case JoinStrictness::Anti:
                settings.writeKeyword("ANTI ");
                break;
        }
    }

    switch (kind)
    {
        case JoinKind::Inner:
            settings.writeKeyword("INNER JOIN");
            break;
        case JoinKind::Left:
            settings.writeKeyword("LEFT JOIN");
            break;
        case JoinKind::Right:
            settings.writeKeyword("RIGHT JOIN");
            break;
        case JoinKind::Full:
            settings.writeKeyword("FULL OUTER JOIN");
            break;
        case JoinKind::Cross:
            settings.writeKeyword("CROSS JOIN");
            break;
        case JoinKind::Comma:
            settings.ostr << ",";
            break;
    }
}


void ASTTableJoin::formatImplAfterTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    frame.expression_list_prepend_whitespace = false;

    if (using_expression_list)
    {
        settings.writeKeyword(" USING ");
        settings.ostr << "(";
        using_expression_list->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }
    else if (on_expression)
    {
        settings.writeKeyword(" ON ");
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
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
    frame.expression_list_prepend_whitespace = true;

    settings.ostr << settings.nl_or_ws << indent_str;
    settings.writeKeyword(kind == Kind::Left ? "LEFT " : "");
    settings.writeKeyword("ARRAY JOIN");

    settings.one_line
        ? expression_list->formatImpl(settings, state, frame)
        : expression_list->as<ASTExpressionList &>().formatImplMultiline(settings, state, frame);
}


void ASTTablesInSelectQueryElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (table_expression)
    {
        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplBeforeTable(settings, state, frame);

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

    for (const auto & child : children)
        child->formatImpl(settings, state, frame);
}

}
