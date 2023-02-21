#include "NamedCollectionsHelpers.h"
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
    NamedCollectionPtr tryGetNamedCollectionFromASTs(ASTs asts, bool throw_unknown_collection)
    {
        if (asts.empty())
            return nullptr;

        const auto * identifier = asts[0]->as<ASTIdentifier>();
        if (!identifier)
            return nullptr;

        const auto & collection_name = identifier->name();
        if (throw_unknown_collection)
            return NamedCollectionFactory::instance().get(collection_name);
        return NamedCollectionFactory::instance().tryGet(collection_name);
    }

    std::optional<std::pair<std::string, Field>> getKeyValueFromAST(ASTPtr ast)
    {
        const auto * function = ast->as<ASTFunction>();
        if (!function || function->name != "equals")
            return std::nullopt;

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        const auto & function_args = function_args_expr->children;

        if (function_args.size() != 2)
            return std::nullopt;

        auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(
            function_args[0], Context::getGlobalContextInstance());
        auto key = checkAndGetLiteralArgument<String>(literal_key, "key");

        auto literal_value = evaluateConstantExpressionOrIdentifierAsLiteral(
            function_args[1], Context::getGlobalContextInstance());
        auto value = literal_value->as<ASTLiteral>()->value;

        return std::pair{key, value};
    }
}


MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(ASTs asts, bool throw_unknown_collection)
{
    if (asts.empty())
        return nullptr;

    auto collection = tryGetNamedCollectionFromASTs(asts, throw_unknown_collection);
    if (!collection)
        return nullptr;

    auto collection_copy = collection->duplicate();

    if (asts.size() == 1)
        return collection_copy;

    for (auto * it = std::next(asts.begin()); it != asts.end(); ++it)
    {
        auto value_override = getKeyValueFromAST(*it);
        if (!value_override && !(*it)->as<ASTFunction>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value argument or function");
        if (!value_override)
            continue;

        const auto & [key, value] = *value_override;
        collection_copy->setOrUpdate<String>(key, toString(value));
    }

    return collection_copy;
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto collection_name = config.getString(config_prefix + ".name", "");
    if (collection_name.empty())
        return nullptr;

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
