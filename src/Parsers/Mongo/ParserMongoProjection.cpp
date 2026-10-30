#include <Parsers/Mongo/ParserMongoProjection.h>

#include <memory>
#include <string_view>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/ParserMongoAggregateExpression.h>
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

namespace
{

/// Whether the value of a member of a projection is a nested document of further members, such as
/// `{"age": 1}` in `{"profile": {"age": 1}}`, rather than an operator or a constant.
bool isNestedProjectionDocument(const rapidjson::Value & value)
{
    if (!value.IsObject())
        return false;
    if (value.MemberCount() == 0)
        return true;
    /// An operator, and an Extended JSON constant such as `{"$date": ...}`, start with `$`.
    return !std::string_view(value.MemberBegin()->name.GetString(), value.MemberBegin()->name.GetStringLength()).starts_with("$");
}

}

bool ParserMongoProjection::addMember(
    const std::string & name, const rapidjson::Value & value, ASTPtr & result, std::vector<std::string> & excluded)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of a projection must not be empty");

    /// A number or a boolean says whether to keep the field; only a document or a `$` prefixed
    /// expression computes one.
    if (value.IsBool() || value.IsNumber())
    {
        const bool included = value.IsBool() ? value.GetBool() : value.GetDouble() != 0;
        /// A nested document is stored as one column per leaf, so the field is kept together
        /// with everything under it: `{"profile": 1}` keeps `profile.name` and `profile.age`.
        if (included)
        {
            auto matcher = make_intrusive<ASTColumnsRegexpMatcher>();
            matcher->setPattern(makeFieldSubtreePattern({name}));
            result->children.push_back(std::move(matcher));
        }
        else
            excluded.push_back(name);
        return true;
    }

    /// The nested form names the fields under a parent, with the same flags at any depth:
    /// `{"profile": {"age": 1}}` is `{"profile.age": 1}`, and `{"profile": {"age": 0}}` is
    /// `{"profile.age": 0}`.
    if (isNestedProjectionDocument(value))
    {
        if (value.MemberCount() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The projection of '{}' must not be an empty document", name);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            const std::string member_name(it->name.GetString(), it->name.GetStringLength());
            if (!addMember(name + "." + member_name, it->value, result, excluded))
                return false;
        }
        return true;
    }

    /// Anything else computes the field with an aggregation expression, as Mongo allows in a
    /// projection of `find`: a string is a literal, `"$a"` refers to the field `a`, and a document
    /// is an operator such as `{"$add": ["$a", 1]}`.
    auto child_node = parseMongoAggregateExpression(value);
    child_node->setAlias(name);
    result->children.push_back(child_node);
    return true;
}

bool ParserMongoProjection::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
    {
        return false;
    }

    ASTPtr result = make_intrusive<ASTExpressionList>();
    std::vector<std::string> excluded;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        if (!addMember(std::string(it->name.GetString(), it->name.GetStringLength()), it->value, result, excluded))
            return false;
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

        /// The transformer is a pattern rather than a list of names, so that excluding a parent
        /// path removes every leaf under it, and excluding a field the collection does not have
        /// is not an error, as in Mongo.
        auto except_transformer = make_intrusive<ASTColumnsExceptTransformer>();
        except_transformer->setPattern(makeFieldSubtreePattern(excluded));

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
