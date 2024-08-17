#include "Parsers/Mongo/ParserMongoOrderBy.h"

#include <memory>

#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/Utils.h>

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

    auto result = std::make_shared<ASTExpressionList>();
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        std::shared_ptr<ASTOrderByElement> element = std::make_shared<ASTOrderByElement>();
        element->direction = it->value.GetInt();
        element->children.push_back(std::make_shared<ASTIdentifier>(it->name.GetString()));
        result->children.push_back(element);
    }

    node = result;
    return true;
}

}

}
