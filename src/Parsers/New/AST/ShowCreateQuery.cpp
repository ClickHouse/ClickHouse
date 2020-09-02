#include <Parsers/New/AST/ShowCreateQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

// static
PtrTo<ShowCreateQuery> ShowCreateQuery::createDatabase(PtrTo<DatabaseIdentifier> identifier)
{
    return PtrTo<ShowCreateQuery>(new ShowCreateQuery(QueryType::DATABASE, {identifier}));
}

// static
PtrTo<ShowCreateQuery> ShowCreateQuery::createTable(bool temporary, PtrTo<TableIdentifier> identifier)
{
    PtrTo<ShowCreateQuery> query(new ShowCreateQuery(QueryType::TABLE, {identifier}));
    query->temporary = temporary;
    return query;
}

ShowCreateQuery::ShowCreateQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;

    (void)query_type, (void)temporary; // TODO
}

}
