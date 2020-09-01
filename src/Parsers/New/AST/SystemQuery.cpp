#include <Parsers/New/AST/SystemQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

// static
PtrTo<SystemQuery> SystemQuery::createSync(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<SystemQuery>(new SystemQuery(QueryType::SYNC, {identifier}));
}

SystemQuery::SystemQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;

    (void)query_type; // TODO
}

}
