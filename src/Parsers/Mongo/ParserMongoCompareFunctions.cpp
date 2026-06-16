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
    auto identifier = make_intrusive<ASTIdentifier>(edge_name);
    if (data.IsInt())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetInt()));
        auto where_condition = makeASTFunction(getFunctionAlias(), identifier, literal);
        node = where_condition;
        return true;
    }
    if (data.IsString())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetString()));
        auto where_condition = makeASTFunction(getFunctionAlias(), identifier, literal);
        node = where_condition;
        return true;
    }
    return false;
}

}

}
