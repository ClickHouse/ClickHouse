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

void ASTDeleteQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DELETE FROM " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        settings.ostr << backQuoteIfNeed(getDatabase());
        settings.ostr << ".";
    }
    settings.ostr << backQuoteIfNeed(getTable());

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
    predicate->formatImpl(settings, state, frame);
}

}
