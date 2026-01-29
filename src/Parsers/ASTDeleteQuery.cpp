#include <Parsers/ASTDeleteQuery.h>


namespace DB
{

String ASTDeleteQuery::getID(char delim) const
{
    return "DeleteQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDeleteQuery::clone() const
{
    auto res = make_intrusive<ASTDeleteQuery>(*this);
    res->children.clear();

    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDeleteQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "DELETE FROM ";

    if (auto db = getDatabaseAst())
    {
        db->format(ostr, settings, state, frame);
        ostr << '.';
    }

    auto tbl = getTableAst();
    chassert(tbl);
    tbl->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (partition)
    {
        ostr << " IN PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    ostr << " WHERE ";
    predicate->format(ostr, settings, state, frame);
}

}
