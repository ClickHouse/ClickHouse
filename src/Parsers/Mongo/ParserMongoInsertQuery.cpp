#include "ParserMongoInsertQuery.h"

#include <memory>

#include <Parsers/Mongo/Utils.h>
#include <Parsers/ParserQuery.h>

namespace DB
{

namespace Mongo
{

/// TODO (scanhex12): add insert queries in mongo
bool ParserMongoInsertManyQuery::parseImpl(ASTPtr & node)
{
    node = nullptr;
    return false;
}

/// TODO (scanhex12): add insert queries in mongo
bool ParserMongoInsertOneQuery::parseImpl(ASTPtr & node)
{
    node = nullptr;
    return false;
}

}

}
