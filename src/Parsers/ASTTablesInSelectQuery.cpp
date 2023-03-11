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


void ASTTableExpression::formatImpl(const FormattingBuffer & out) const
{
    out.setCurrentSelect(this);

    if (database_and_table_name)
    {
        out.ostr << " ";
        database_and_table_name->formatImpl(out);
    }
    else if (table_function && !(table_function->as<ASTFunction>()->prefer_subquery_to_function_formatting && subquery))
    {
        out.ostr << " ";
        table_function->formatImpl(out);
    }
    else if (subquery)
    {
        out.nlOrWs();
        out.writeIndent();
        subquery->formatImpl(out);
    }

    if (final)
    {
        out.nlOrWs();
        out.writeIndent();
        out.writeKeyword("FINAL");
    }

    if (sample_size)
    {
        out.nlOrWs();
        out.writeIndent();
        out.writeKeyword("SAMPLE ");
        sample_size->formatImpl(out);

        if (sample_offset)
        {
            out.writeKeyword(" OFFSET ");
            sample_offset->formatImpl(out);
        }
    }
}


void ASTTableJoin::formatImplBeforeTable(const FormattingBuffer & out) const
{
    if (kind != JoinKind::Comma)
    {
        out.nlOrWs();
        out.writeIndent();
    }

    switch (locality)
    {
        case JoinLocality::Unspecified:
        case JoinLocality::Local:
            break;
        case JoinLocality::Global:
            out.writeKeyword("GLOBAL ");
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
                out.writeKeyword("ANY ");
                break;
            case JoinStrictness::All:
                out.writeKeyword("ALL ");
                break;
            case JoinStrictness::Asof:
                out.writeKeyword("ASOF ");
                break;
            case JoinStrictness::Semi:
                out.writeKeyword("SEMI ");
                break;
            case JoinStrictness::Anti:
                out.writeKeyword("ANTI ");
                break;
        }
    }

    switch (kind)
    {
        case JoinKind::Inner:
            out.writeKeyword("INNER JOIN");
            break;
        case JoinKind::Left:
            out.writeKeyword("LEFT JOIN");
            break;
        case JoinKind::Right:
            out.writeKeyword("RIGHT JOIN");
            break;
        case JoinKind::Full:
            out.writeKeyword("FULL OUTER JOIN");
            break;
        case JoinKind::Cross:
            out.writeKeyword("CROSS JOIN");
            break;
        case JoinKind::Comma:
            out.ostr << ",";
            break;
    }
}


void ASTTableJoin::formatImplAfterTable(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);
    out.setExpressionListPrependWhitespace(false);

    if (using_expression_list)
    {
        out.writeKeyword(" USING ");
        out.ostr << "(";
        using_expression_list->formatImpl(out);
        out.ostr << ")";
    }
    else if (on_expression)
    {
        out.writeKeyword(" ON ");
        on_expression->formatImpl(out);
    }
}


void ASTTableJoin::formatImpl(const FormattingBuffer & out) const
{
    formatImplBeforeTable(out);
    out.ostr << " ... ";
    formatImplAfterTable(out);
}


void ASTArrayJoin::formatImpl(const FormattingBuffer & out) const
{
    out.setExpressionListPrependWhitespace();

    out.nlOrWs();
    out.writeIndent();
    out.writeKeyword(kind == Kind::Left ? "LEFT " : "");
    out.writeKeyword("ARRAY JOIN");

    out.isOneLine()
        ? expression_list->formatImpl(out)
        : expression_list->as<ASTExpressionList &>().formatImplMultiline(out);
}


void ASTTablesInSelectQueryElement::formatImpl(const FormattingBuffer & out) const
{
    if (table_expression)
    {
        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplBeforeTable(out);

        table_expression->formatImpl(out);

        if (table_join)
            table_join->as<ASTTableJoin &>().formatImplAfterTable(out);
    }
    else if (array_join)
    {
        array_join->formatImpl(out);
    }
}


void ASTTablesInSelectQuery::formatImpl(const FormattingBuffer & out) const
{
    for (const auto & child : children)
        child->formatImpl(out);
}

}
