#include "NamedCollectionsHelpers.h"
#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::optional<std::string> getCollectionName(ASTs asts)
    {
        if (asts.empty())
            return std::nullopt;

        const auto * identifier = asts[0]->as<ASTIdentifier>();
        if (!identifier)
            return std::nullopt;

        return identifier->name();
    }

    std::optional<std::pair<std::string, std::variant<Field, ASTPtr>>> getKeyValueFromAST(ASTPtr ast, bool fallback_to_ast_value, ContextPtr context)
    {
        const auto * function = ast->as<ASTFunction>();
        if (!function || function->name != "equals")
            return std::nullopt;

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        const auto & function_args = function_args_expr->children;

        if (function_args.size() != 2)
            return std::nullopt;

        auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[0], context);
        auto key = checkAndGetLiteralArgument<String>(literal_key, "key");

        ASTPtr literal_value;
        try
        {
            if (key == "database" || key == "db")
                literal_value = evaluateConstantExpressionForDatabaseName(function_args[1], context);
            else
                literal_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        }
        catch (...)
        {
            if (fallback_to_ast_value)
                return std::pair{key, function_args[1]};
            throw;
        }

        auto value = literal_value->as<ASTLiteral>()->value;
        return std::pair{key, Field(value)};
    }
}


MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    ASTs asts, ContextPtr context, bool throw_unknown_collection, std::vector<std::pair<std::string, ASTPtr>> * complex_args)
{
    if (asts.empty())
        return nullptr;

    NamedCollectionUtils::loadIfNot();

    auto collection_name = getCollectionName(asts);
    if (!collection_name.has_value())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, *collection_name);

    NamedCollectionPtr collection;
    if (throw_unknown_collection)
        collection = NamedCollectionFactory::instance().get(*collection_name);
    else
        collection = NamedCollectionFactory::instance().tryGet(*collection_name);

    if (!collection)
        return nullptr;

    auto collection_copy = collection->duplicate();

    if (asts.size() == 1)
        return collection_copy;

    for (auto * it = std::next(asts.begin()); it != asts.end(); ++it)
    {
        auto value_override = getKeyValueFromAST(*it, /* fallback_to_ast_value */complex_args != nullptr, context);

        if (!value_override && !(*it)->as<ASTFunction>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value argument or function");
        if (!value_override)
            continue;

        if (const ASTPtr * value = std::get_if<ASTPtr>(&value_override->second))
        {
            complex_args->emplace_back(value_override->first, *value);
            continue;
        }

        const auto & [key, value] = *value_override;
        collection_copy->setOrUpdate<String>(key, toString(std::get<Field>(value_override->second)));
    }

    return collection_copy;
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto collection_name = config.getString(config_prefix + ".name", "");
    if (collection_name.empty())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, collection_name);

    const auto & collection = NamedCollectionFactory::instance().get(collection_name);
    auto collection_copy = collection->duplicate();

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
        collection_copy->setOrUpdate<String>(key, config.getString(config_prefix + '.' + key));

    return collection_copy;
}

HTTPHeaderEntries getHeadersFromNamedCollection(const NamedCollection & collection)
{
    HTTPHeaderEntries headers;
    auto keys = collection.getKeys(0, "headers");
    for (const auto & key : keys)
        headers.emplace_back(collection.get<String>(key + ".name"), collection.get<String>(key + ".value"));
    return headers;
}

}
