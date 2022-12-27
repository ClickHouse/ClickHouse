#include "NamedCollectionsHelpers.h"
#include <Storages/NamedCollections/NamedCollections.h>
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
    NamedCollectionPtr tryGetNamedCollectionFromASTs(ASTs asts)
    {
        if (asts.empty())
            return nullptr;

        const auto * identifier = asts[0]->as<ASTIdentifier>();
        if (!identifier)
            return nullptr;

        const auto & collection_name = identifier->name();
        return NamedCollectionFactory::instance().get(collection_name);
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


NamedCollectionPtr tryGetNamedCollectionWithOverrides(ASTs asts)
{
    if (asts.empty())
        return nullptr;

    auto collection = tryGetNamedCollectionFromASTs(asts);
    if (!collection)
        return nullptr;

    if (asts.size() == 1)
        return collection;

    auto collection_copy = collection->duplicate();

    for (const auto & ast : asts)
    {
        auto value_override = getKeyValueFromAST(ast);
        if (!value_override)
            continue;

        const auto & [key, value] = *value_override;
        collection_copy->setOrUpdate<String>(key, toString(value));
    }

    return collection_copy;
}

void validateNamedCollection(
    const NamedCollection & collection,
    const std::unordered_set<std::string_view> & required_keys,
    const std::unordered_set<std::string_view> & optional_keys)
{
    const auto & keys = collection.getKeys();
    for (const auto & key : keys)
    {
        if (!required_keys.contains(key) && !optional_keys.contains(key))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected key `{}` in named collection. Required keys: {}, optional keys: {}",
                key, fmt::join(required_keys, ", "), fmt::join(optional_keys, ", "));
        }
    }

    for (const auto & key : required_keys)
    {
        if (!keys.contains(key))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Key `{}` is required, but not specified. Required keys: {}, optional keys: {}",
                key, fmt::join(required_keys, ", "), fmt::join(optional_keys, ", "));
        }
    }
}

}
