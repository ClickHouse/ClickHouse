#include <Parsers/Mongo/ParserMongoFilter.h>

#include <memory>
#include <string_view>

#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoFilter::parseImpl(ASTPtr & node)
{
    chassert(data.IsObject());
    std::vector<ASTPtr> child_trees;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        /// `$comment` documents the query for the profiler and holds no condition.
        if (std::string_view(it->name.GetString(), it->name.GetStringLength()) == "$comment")
            continue;

        auto parser = createParser(copyValue(it->value, metadata->getAllocator()), metadata, it->name.GetString());
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        child_trees.push_back(child_node);
    }

    if (child_trees.empty())
    {
        return true;
    }

    if (child_trees.size() == 1)
    {
        node = child_trees[0];
        return true;
    }

    auto result = makeASTFunction("and");
    for (const auto & elem : child_trees)
    {
        result->arguments->children.push_back(elem);
    }
    node = result;
    return true;
}

}

}
