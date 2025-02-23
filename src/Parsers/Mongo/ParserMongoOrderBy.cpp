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
    std::cerr << "ParserMongoOrderBy beg\n";
    if (!data.IsObject())
    {
        return false;
    }
    std::cerr << "ParserMongoOrderBy\n";

    auto result = std::make_shared<ASTExpressionList>();
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        std::cerr << "order " << it->name.GetString() << '\n';
        std::shared_ptr<ASTOrderByElement> element = std::make_shared<ASTOrderByElement>();
        element->direction = it->value.GetInt();
        element->children.push_back(std::make_shared<ASTIdentifier>(it->name.GetString()));
        result->children.push_back(element);
    }
    std::cerr << "OK\n";
    node = result;
    return true;
}

}

}
