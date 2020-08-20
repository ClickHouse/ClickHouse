#include <Parsers/New/AST/DropQuery.h>

#include <Parsers/New/AST/Identifier.h>

#include <Parsers/ASTDropQuery.h>


namespace DB::AST
{

// static
PtrTo<DropQuery> DropQuery::createDropDatabase(bool if_exists, PtrTo<DatabaseIdentifier> identifier)
{
    auto query = PtrTo<DropQuery>(new DropQuery(QueryType::DATABASE, {identifier}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<DropQuery> DropQuery::createDropTable(bool if_exists, bool temporary, PtrTo<TableIdentifier> identifier)
{
    auto query = PtrTo<DropQuery>(new DropQuery(QueryType::TABLE, {identifier}));
    query->if_exists = if_exists;
    query->temporary = temporary;
    return query;
}

DropQuery::DropQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;
}

ASTPtr DropQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDropQuery>();

    query->kind = ASTDropQuery::Drop;
    query->if_exists = if_exists;
    query->temporary = temporary;

    // TODO: refactor |ASTQueryWithTableAndOutput| to accept |ASTIdentifier|
    switch(query_type)
    {
        case QueryType::DATABASE:
            query->database = children[NAME]->as<DatabaseIdentifier>()->getName();
            break;
        case QueryType::TABLE:
        {
            query->table = children[NAME]->as<TableIdentifier>()->getName();
            if (auto database = children[NAME]->as<TableIdentifier>()->getDatabase())
                query->database = database->getName();
            break;
        }
    }

    convertToOldPartially(query);

    return query;
}

}
