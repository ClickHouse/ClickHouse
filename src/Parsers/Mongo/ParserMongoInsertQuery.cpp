#include "ParserMongoInsertQuery.h"
#include <memory>
#include <stdexcept>
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTInsertQuery.h"
#include "Parsers/parseQuery.h"
#include <iostream>
#include <Parsers/ParserQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoInsertManyQuery::parseImpl(ASTPtr & node)
{
    node = nullptr;
    return false;
}

bool ParserMongoInsertOneQuery::parseImpl(ASTPtr & node)
{
    node = nullptr;
    return false;
}

}

}
