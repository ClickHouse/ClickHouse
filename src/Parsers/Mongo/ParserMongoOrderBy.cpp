#include "Parsers/Mongo/ParserMongoOrderBy.h"

#include <memory>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/Utils.h>

#include <iostream>

namespace DB
{

namespace Mongo
{

bool ParserMongoOrderBy::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
    {
        return false;
    }

    auto result = make_intrusive<ASTExpressionList>();
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        auto element = make_intrusive<ASTOrderByElement>();
        element->direction = it->value.GetInt();
        element->children.push_back(make_intrusive<ASTIdentifier>(it->name.GetString()));
        result->children.push_back(element);
    }
    node = result;
    return true;
}

}

}
