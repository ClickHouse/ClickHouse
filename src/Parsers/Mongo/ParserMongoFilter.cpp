#include <Parsers/Mongo/ParserMongoFilter.h>

#include <memory>
#include <string_view>

#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTFunction.h>
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

bool ParserMongoFilter::parseImpl(ASTPtr & node)
{
    /** A filter is a document. Anything else - `{"$or": [{"a": 1}, true]}` names a boolean as one
      * of the filters to combine - would be walked as one all the same, because iterating the
      * members of a value that is not an object is undefined and an assertion does not hold in a
      * release build.
      */
    if (!data.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A filter must be a document");

    std::vector<ASTPtr> child_trees;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        std::string_view name(it->name.GetString(), it->name.GetStringLength());

        /// `$comment` documents the query for the profiler and holds no condition.
        if (name == "$comment")
            continue;

        /// An empty name is no field, and an identifier without a name makes an AST that the rest
        /// of the server cannot walk.
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A filter must not name an empty field");

        auto parser = createParser(copyValue(it->value, metadata->getAllocator()), metadata, it->name.GetString());
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }

        /// A parser answers with no node when the document it was given holds no condition, which
        /// a list of filters must not turn into a hole in the tree it builds.
        if (!child_node)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The filter of the field '{}' holds no condition", name);

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
