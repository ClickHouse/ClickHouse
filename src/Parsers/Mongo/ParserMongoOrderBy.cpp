#include <Parsers/Mongo/ParserMongoOrderBy.h>

#include <memory>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/Utils.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

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
        if (!it->value.IsInt64() || (it->value.GetInt64() != 1 && it->value.GetInt64() != -1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The direction of the sort on '{}' must be 1 or -1", it->name.GetString());

        const int direction = static_cast<int>(it->value.GetInt64());
        auto element = make_intrusive<ASTOrderByElement>();
        element->direction = direction;
        element->nulls_direction = direction;
        element->children.push_back(make_intrusive<ASTIdentifier>(it->name.GetString()));
        result->children.push_back(element);
    }
    node = result;
    return true;
}

}

}
