#include "Parsers/Mongo/ParserMongoProjection.h"

#include <memory>

#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoProjection::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
    {
        return false;
    }

    auto result = std::make_shared<ASTExpressionList>();
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        if (it->value.IsInt())
        {
            auto include_field = it->value.GetInt();
            if (include_field)
            {
                result->children.push_back(std::make_shared<ASTIdentifier>(it->name.GetString()));
            }
            continue;
        }
        ASTPtr child_node;
        auto parser = createParser(copyValue(it->value), metadata, it->name.GetString(), true);
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        child_node->setAlias(it->name.GetString());
        result->children.push_back(child_node);
    }

    node = result;
    return true;
}

}

}
