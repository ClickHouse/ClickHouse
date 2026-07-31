#include <Backups/BackupInfo.h>

#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <map>
#include <optional>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_named_collection_override_by_default;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String BackupInfo::toString() const
{
    ASTPtr ast = toAST();
    return ast->formatWithSecretsOneLine();
}


BackupInfo BackupInfo::fromString(const String & str)
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return fromAST(*ast);
}


namespace
{
    /// Check if an AST node is a key-value assignment (e.g., url='...' parsed as equals(url, '...'))
    bool isKeyValueArg(const ASTPtr & ast)
    {
        const auto * func = ast->as<const ASTFunction>();
        return func && func->name == "equals";
    }

}

ASTPtr BackupInfo::toAST() const
{
    auto func = make_intrusive<ASTFunction>();
    func->name = backup_engine_name;
    func->setKind(ASTFunction::Kind::BACKUP_NAME);

    auto list = make_intrusive<ASTExpressionList>();
    func->arguments = list;
    func->children.push_back(list);
    list->children.reserve(args.size() + kv_args.size() + !id_arg.empty());

    if (!id_arg.empty())
        list->children.push_back(make_intrusive<ASTIdentifier>(id_arg));

    for (const auto & arg : args)
        list->children.push_back(make_intrusive<ASTLiteral>(arg));

    for (const auto & kv_arg : kv_args)
        list->children.push_back(kv_arg);

    if (function_arg)
        list->children.push_back(function_arg);

    func->setNoEmptyArgs(true);
    return func;
}


BackupInfo BackupInfo::fromAST(const IAST & ast)
{
    const auto * func = ast.as<const ASTFunction>();
    if (!func)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected function, got {}", ast.formatForErrorMessage());

    BackupInfo res;
    res.backup_engine_name = func->name;

    if (func->arguments)
    {
        const auto * list = func->arguments->as<const ASTExpressionList>();
        if (!list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected list, got {}", func->arguments->formatForErrorMessage());

        size_t index = 0;
        if (!list->children.empty())
        {
            const auto * id = list->children[0]->as<const ASTIdentifier>();
            if (id)
            {
                res.id_arg = id->name();
                ++index;
            }
        }

        size_t args_size = list->children.size();
        res.args.reserve(args_size - index);
        for (; index < args_size; ++index)
        {
            const auto & elem = list->children[index];

            /// Check for key-value arguments (e.g., url='...' parsed as equals(url, '...'))
            if (isKeyValueArg(elem))
            {
                res.kv_args.push_back(elem);
                continue;
            }

            const auto * lit = elem->as<const ASTLiteral>();
            if (!lit)
            {
                if (index == args_size - 1 && elem->as<const ASTFunction>())
                {
                    res.function_arg = elem;
                    break;
                }
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected literal, got {}", elem->formatForErrorMessage());
            }
            res.args.push_back(lit->value);
        }
    }

    return res;
}


String BackupInfo::toStringForLogging() const
{
    return toAST()->formatForLogging();
}

String BackupInfo::evaluateKeyValueArgument(const ASTPtr & kv_arg, size_t index, ContextPtr context)
{
    const auto * function = kv_arg ? kv_arg->as<const ASTFunction>() : nullptr;
    const auto * arguments = function && function->arguments ? function->arguments->as<const ASTExpressionList>() : nullptr;
    if (!function || function->name != "equals" || !arguments || arguments->children.size() != 2 || index >= 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid backup locator key-value argument");

    try
    {
        ASTPtr evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(arguments->children[index], context);
        const auto * literal = evaluated->as<const ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a string");
        return literal->value.safeGet<String>();
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup locator key-value argument must be a constant string");
    }
}

bool BackupInfo::isEquivalentTo(const BackupInfo & other, ContextPtr context) const
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Context is required to compare backup locators");

    /// Named collection overrides are an unordered key-value map, but all other locator syntax must match exactly.
    BackupInfo lhs = *this;
    BackupInfo rhs = other;
    lhs.kv_args.clear();
    rhs.kv_args.clear();
    if (lhs.toString() != rhs.toString())
        return false;

    auto get_key_values = [&](const ASTs & key_value_args)
    {
        std::map<String, String> key_values;
        for (const auto & arg : key_value_args)
        {
            String key;
            String value;
            try
            {
                auto key_value = getKeyValueFromAST(arg, context);
                key = std::move(key_value.first);
                value = fieldToString(key_value.second);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup locator key-value argument must be constant");
            }
            if (!key_values.emplace(std::move(key), std::move(value)).second)
                return std::optional<std::map<String, String>>{};
        }
        return std::optional{std::move(key_values)};
    };

    const auto lhs_key_values = get_key_values(kv_args);
    const auto rhs_key_values = get_key_values(other.kv_args);
    return lhs_key_values && rhs_key_values && lhs_key_values == rhs_key_values;
}


NamedCollectionPtr BackupInfo::getNamedCollection(ContextPtr context) const
{
    if (id_arg.empty())
        return nullptr;

    if (frozen_named_collection)
    {
        context->checkAccess(AccessType::NAMED_COLLECTION, id_arg);
        return frozen_named_collection;
    }

    /// Load named collections (both from config and SQL-defined)
    NamedCollectionFactory::instance().loadIfNot();

    auto collection = NamedCollectionFactory::instance().tryGet(id_arg);
    if (!collection)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no named collection `{}`", id_arg);

    /// Check access rights for the named collection
    context->checkAccess(AccessType::NAMED_COLLECTION, id_arg);

    /// Apply key-value overrides from the query (e.g., url='...', blob_path='...')
    if (!kv_args.empty())
    {
        auto mutable_collection = collection->duplicate();
        auto params_from_query = getParamsMapFromAST(kv_args, context);
        const auto allow_override_by_default = context->getSettingsRef()[Setting::allow_named_collection_override_by_default];
        for (const auto & [key, value] : params_from_query)
        {
            /// Enforce the same override permission as the table-function/storage paths
            /// (`tryGetNamedCollectionWithOverrides`): a non-overridable key (e.g. an operator-static endpoint or
            /// credentials) must not be redirected from the query, otherwise the collection's static credentials
            /// could be reused against a user-chosen endpoint under the S3 credential restriction.
            if (!mutable_collection->isOverridable(key, allow_override_by_default))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", key);
            mutable_collection->setOrUpdate<String>(key, fieldToString(value), {});
            mutable_collection->markQueryOverridden(key);
        }
        collection = std::move(mutable_collection);
    }

    return collection;
}

BackupInfo BackupInfo::freezeNamedCollection(ContextPtr context) const
{
    if (id_arg.empty() || frozen_named_collection)
        return *this;

    BackupInfo res = *this;
    auto collection = getNamedCollection(context);
    /// `getNamedCollection` already returns a private copy when overrides are present.
    res.frozen_named_collection = kv_args.empty() ? collection->duplicate() : std::move(collection);
    return res;
}

}
