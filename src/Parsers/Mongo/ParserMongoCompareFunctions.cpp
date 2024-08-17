#include "ParserMongoCompareFunctions.h"

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace Mongo
{

bool ICompareFunction::parseImpl(ASTPtr & node)
{
    auto identifier = std::make_shared<ASTIdentifier>(edge_name);
    if (data.IsInt())
    {
        auto literal = std::make_shared<ASTLiteral>(Field(data.GetInt()));
        auto where_condition = makeASTFunction(getFunctionAlias(), identifier, literal);
        node = where_condition;
        return true;
    }
    if (data.IsString())
    {
        auto literal = std::make_shared<ASTLiteral>(Field(data.GetString()));
        auto where_condition = makeASTFunction(getFunctionAlias(), identifier, literal);
        node = where_condition;
        return true;
    }
    return false;
}

}

}
