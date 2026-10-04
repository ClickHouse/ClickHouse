#include <Parsers/Mongo/ParserMongoDeleteQuery.h>

#include <rapidjson/document.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoOrderBy.h>
#include <Parsers/Mongo/ParserMongoProjection.h>
#include <Parsers/Mongo/Utils.h>
#include <Parsers/ASTDeleteQuery.h>


#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoDeleteQuery::parseImpl(ASTPtr & node)
{
    auto delete_query = make_intrusive<ASTDeleteQuery>();
    node = delete_query;

    /// `set` is what puts the identifier into `children`, which `ASTQueryWithTableAndOutput`
    /// requires: `InterpreterDeleteQuery` fills in the database with `setDatabase`, which
    /// starts by removing the current one from `children`.
    delete_query->set(delete_query->table, make_intrusive<ASTIdentifier>(metadata->getCollectionName()));
    if (!metadata->getDatabaseName().empty())
        delete_query->set(delete_query->database, make_intrusive<ASTIdentifier>(metadata->getDatabaseName()));

    /// Traverse data tree for WHERE operator
    ASTPtr where_condition;

    if (!ParserMongoFilter(std::move(data), metadata, "").parseImpl(where_condition))
        return false;

    /** An empty filter - `db.t.deleteMany({})`, which deletes every document - leaves no condition
      * behind, and `ASTDeleteQuery` has no notion of a delete without one: `formatQueryImpl` and
      * `InterpreterDeleteQuery` both walk `predicate` unconditionally, so a null one is a
      * segmentation fault any client can ask for. `DELETE FROM ... WHERE 1` says the same thing,
      * and is what the update path already does with an empty filter.
      */
    if (!where_condition)
        where_condition = make_intrusive<ASTLiteral>(Field(UInt64(1)));

    delete_query->predicate = std::move(where_condition);
    return true;
}

}

}
