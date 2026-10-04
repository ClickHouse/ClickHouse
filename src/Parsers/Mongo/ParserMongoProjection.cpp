#include <Parsers/Mongo/ParserMongoProjection.h>

#include <memory>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

bool ParserMongoProjection::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
    {
        return false;
    }

    auto result = make_intrusive<ASTExpressionList>();
    std::vector<std::string> excluded;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        std::string name = it->name.GetString();
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of a projection must not be empty");

        /// A number or a boolean says whether to keep the field; only a document or a `$` prefixed
        /// expression computes one.
        if (it->value.IsBool() || it->value.IsNumber())
        {
            const bool included = it->value.IsBool() ? it->value.GetBool() : it->value.GetDouble() != 0;
            /// A field of a document is answered together with the fields below it: the nested
            /// document of a table is a set of columns whose names are the dotted paths of its
            /// fields, so `{"profile": 1}` asks for `profile` and for every `profile.<...>`.
            if (included)
                result->children.push_back(makeFieldSubtreeMatcher(name));
            else
                excluded.push_back(std::move(name));
            continue;
        }
        ASTPtr child_node;
        auto parser = createParser(copyValue(it->value, metadata->getAllocator()), metadata, name, true);
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        child_node->setAlias(name);
        result->children.push_back(child_node);
    }

    if (!excluded.empty() && !result->children.empty())
    {
        /// Mongo rejects an exclusion inside an inclusion projection, with one exception: the
        /// implicit `_id` may always be suppressed, and `{"name": 1, "_id": 0}` is the usual way
        /// to ask for "only these fields". This dialect never adds an implicit `_id`, so the
        /// exclusion has nothing left to do and is simply dropped.
        std::erase(excluded, "_id");
        if (!excluded.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "A projection must not mix inclusion and exclusion of fields, except an exclusion of '_id'");
    }

    if (!excluded.empty())
    {
        /// A projection either names the fields to keep or becomes a `* EXCEPT (...)`.

        auto asterisk = make_intrusive<ASTAsterisk>();

        /// The transformer is not strict on purpose: excluding a field the collection does not
        /// have is not an error in Mongo. It matches by pattern, so that an excluded field takes
        /// the fields below it with it, the way an included one brings them along.
        auto except_transformer = make_intrusive<ASTColumnsExceptTransformer>();
        String pattern;
        for (const auto & name : excluded)
            pattern += (pattern.empty() ? "" : "|") + fieldSubtreePattern(name);
        except_transformer->setPattern(std::move(pattern));

        auto transformers = make_intrusive<ASTColumnsTransformerList>();
        transformers->children.push_back(std::move(except_transformer));

        asterisk->transformers = transformers;
        asterisk->children.push_back(std::move(transformers));
        result->children.push_back(std::move(asterisk));
    }

    /// An empty projection document asks for the whole document.
    if (result->children.empty())
        result->children.push_back(make_intrusive<ASTAsterisk>());

    node = result;
    return true;
}

}

}
