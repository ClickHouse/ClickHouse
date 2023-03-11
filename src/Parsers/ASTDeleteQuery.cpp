#include <Parsers/ASTDeleteQuery.h>
#include <Common/quoteString.h>

namespace DB
{

String ASTDeleteQuery::getID(char delim) const
{
    return "DeleteQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDeleteQuery::clone() const
{
    auto res = std::make_shared<ASTDeleteQuery>(*this);
    res->children.clear();

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    if (settings_ast)
    {
        res->settings_ast = settings_ast->clone();
        res->children.push_back(res->settings_ast);
    }

    cloneTableOptions(*res);
    return res;
}

void ASTDeleteQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("DELETE FROM ");

    if (database)
    {
        out.ostr << backQuoteIfNeed(getDatabase());
        out.ostr << ".";
    }
    out.ostr << backQuoteIfNeed(getTable());

    out.writeKeyword(" WHERE ");
    predicate->formatImpl(out);
}

}
