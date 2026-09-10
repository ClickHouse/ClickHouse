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
#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Parsers/parseQuery.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Poco/URI.h>

#include <algorithm>
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

    /// Returns the key of a key-value assignment (e.g., "url" for url='...').
    std::optional<String> getKeyValueArgName(const ASTPtr & ast)
    {
        const auto * func = ast->as<const ASTFunction>();
        if (!func || func->name != "equals")
            return std::nullopt;

        const auto * list = func->arguments ? func->arguments->as<const ASTExpressionList>() : nullptr;
        if (!list || list->children.size() != 2)
            return std::nullopt;

        if (auto name = tryGetIdentifierName(list->children[0]))
            return name;

        const auto * literal = list->children[0]->as<const ASTLiteral>();
        if (literal && literal->value.getType() == Field::Types::Which::String)
            return literal->value.safeGet<String>();

        return std::nullopt;
    }

    /// Returns the effective key of a key-value assignment, evaluating a constant expression key with
    /// the context the same way `getKeyValueFromAST` (the named collection opening path) does. This is
    /// needed so that keys written as expressions (e.g. concat('secret_', 'access_key')) are classified
    /// correctly and their credentials are not persisted into the backup metadata. Without a context a
    /// non-literal key is rejected (fail closed).
    std::optional<String> getEffectiveKeyValueArgName(const ASTPtr & ast, const ContextPtr & context)
    {
        if (auto name = getKeyValueArgName(ast))
            return name;

        const auto * func = ast->as<const ASTFunction>();
        if (!func || func->name != "equals")
            return std::nullopt;

        const auto * list = func->arguments ? func->arguments->as<const ASTExpressionList>() : nullptr;
        if (!list || list->children.size() != 2)
            return std::nullopt;

        if (!context)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove credentials from the base backup locator with a non-literal argument key");

        ASTPtr evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(list->children[0], context);
        const auto * literal = evaluated->as<const ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The base backup locator argument key must be a constant string expression");
        return literal->value.safeGet<String>();
    }

    /// Returns the value of a key-value assignment when it is a string literal (e.g., "..." for url='...').
    std::optional<String> getKeyValueArgStringValue(const ASTPtr & ast)
    {
        const auto * func = ast->as<const ASTFunction>();
        if (!func || func->name != "equals")
            return std::nullopt;

        const auto * list = func->arguments ? func->arguments->as<const ASTExpressionList>() : nullptr;
        if (!list || list->children.size() != 2)
            return std::nullopt;

        const auto * literal = list->children[1]->as<const ASTLiteral>();
        if (literal && literal->value.getType() == Field::Types::Which::String)
            return literal->value.safeGet<String>();

        return std::nullopt;
    }

    /// Returns the effective value of a key-value assignment, resolving a constant expression with the
    /// context the same way `S3StorageParsedArguments::collectCredentials` resolves it when opening the
    /// locator. A value written as an expression (e.g. concat('arn::', 'role')) is therefore classified
    /// by what it loads as. Without a context, or when it is no constant string, it is rejected.
    std::optional<String> getEffectiveKeyValueArgStringValue(const ASTPtr & ast, const ContextPtr & context)
    {
        if (auto value = getKeyValueArgStringValue(ast))
            return value;

        const auto * func = ast->as<const ASTFunction>();
        if (!func || func->name != "equals")
            return std::nullopt;

        const auto * list = func->arguments ? func->arguments->as<const ASTExpressionList>() : nullptr;
        if (!list || list->children.size() != 2)
            return std::nullopt;

        if (!context)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Cannot remove credentials from the base backup locator with a non-literal argument value");

        ASTPtr evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(list->children[1], context);
        const auto * evaluated_literal = evaluated->as<const ASTLiteral>();
        if (!evaluated_literal || evaluated_literal->value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The base backup locator argument value must be a constant string expression");
        return evaluated_literal->value.safeGet<String>();
    }

    /// Rebuilds an `extra_credentials(...)` keeping only its non-secret keys, or `nullptr` if none are
    /// left. `role_arn` and `role_session_name` only name the role to assume, which grants nothing
    /// without the server's own identity and a matching trust policy, so `isNonSecretExtraCredentialsKey`
    /// keeps them -- the same predicate that keeps them visible in a logged query. `external_id` is the
    /// shared secret of the triple, and anything unclassifiable is dropped.
    ASTPtr withoutSecretExtraCredentials(const ASTPtr & function_arg, const ContextPtr & context)
    {
        const auto * func = function_arg->as<const ASTFunction>();
        /// Only `extra_credentials` is consumed by `registerBackupEngineS3`; don't guess at anything else.
        if (!func || func->name != "extra_credentials" || !func->arguments)
            return nullptr;

        ASTs kept;
        for (const auto & child : func->arguments->children)
        {
            auto key = getEffectiveKeyValueArgName(child, context);
            if (!key || !FunctionSecretArgumentsFinder::isNonSecretExtraCredentialsKey(*key))
                continue;
            /// The value is only classified, not rewritten: an expression the open path resolves stays as
            /// it was written, so a locator that loses nothing serializes byte for byte.
            if (!getEffectiveKeyValueArgStringValue(child, context))
                continue;
            kept.push_back(child);
        }

        if (kept.empty())
            return nullptr;

        /// Keep the original node when nothing is dropped, so the locator serializes byte for byte.
        if (kept.size() == func->arguments->children.size())
            return function_arg;

        /// The AST may be shared with the query, so it must not be modified in place.
        auto res = make_intrusive<ASTFunction>();
        res->name = func->name;
        res->arguments = make_intrusive<ASTExpressionList>();
        res->arguments->children = std::move(kept);
        res->children.push_back(res->arguments);
        return res;
    }

    /// Whether the trailing function is an `extra_credentials(...)` clause that authenticates. Only a
    /// non-empty `role_arn` makes `getCredentialsProvider` wrap the provider chain in the STS assume-role
    /// provider, so a clause carrying only `role_session_name` or `external_id` names no identity to lend
    /// and is not credentials to copy. No other trailing function is carried over either.
    /// Keys and values are resolved with the context the way `collectCredentials` resolves them when the
    /// locator is opened, so every spelling is classified by what it loads as rather than how it is
    /// written; without a context only literals can be read and anything else is not lent.
    bool hasRoleToAssume(const ASTPtr & function_arg, const ContextPtr & context)
    {
        const auto * func = function_arg ? function_arg->as<const ASTFunction>() : nullptr;
        if (!func || func->name != "extra_credentials" || !func->arguments)
            return false;

        for (const auto & child : func->arguments->children)
        {
            auto key = context ? getEffectiveKeyValueArgName(child, context) : getKeyValueArgName(child);
            if (!key || *key != "role_arn")
                continue;
            auto value = context ? getEffectiveKeyValueArgStringValue(child, context) : getKeyValueArgStringValue(child);
            return value && !value->empty();
        }

        return false;
    }

    /// Removes the credentials embedded in the URL itself: the userinfo part and the authentication
    /// query parameters of presigned URLs (the same set as recognized by `S3::URI`).
    /// Destination-significant parameters such as `versionId` are kept.
    /// Returns the URL unchanged if there is nothing to remove.
    String removeCredentialsFromS3URL(const String & url)
    {
        if (!url.contains('@') && !url.contains('?'))
            return url;

        Poco::URI uri(url);

        bool changed = false;
        if (!uri.getUserInfo().empty())
        {
            uri.setUserInfo("");
            changed = true;
        }

        Poco::URI::QueryParameters kept_params;
        for (const auto & [key, value] : uri.getQueryParameters())
        {
            if (key == "AWSAccessKeyId" || key == "Signature" || key == "Expires" || key.starts_with("X-Amz-")
                || key == "GoogleAccessId" || key.starts_with("X-Goog-"))
                changed = true;
            else
                kept_params.push_back({key, value});
        }

        if (!changed)
            return url;

        uri.setQueryParameters(kept_params);
        return uri.toString();
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

bool BackupInfo::canCopyS3CredentialsTo(const BackupInfo & dest, ContextPtr context) const
{
    /// Must mirror the conditions checked by `copyS3CredentialsTo`.
    return id_arg.empty() && dest.id_arg.empty()
        && backup_engine_name == "S3" && dest.backup_engine_name == "S3"
        && (args.size() == 3 || hasRoleToAssume(function_arg, context));
}

void BackupInfo::copyS3CredentialsTo(BackupInfo & dest, ContextPtr context) const
{
    /// named_collection case, no need to update
    if (!dest.id_arg.empty() || !id_arg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup is not compatible with named_collections");

    if (backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", toStringForLogging());
    if (dest.backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", dest.toStringForLogging());

    /// An `S3` locator authenticates with a positional key pair or with a role to assume. Both are
    /// carried over, so a role-authenticated backup can lend its credentials the way a key pair does.
    /// A clause naming no role authenticates nothing and is rejected, the same way a locator carrying
    /// no credentials at all is.
    const bool has_key_pair = args.size() == 3;
    const bool has_role_to_assume = hasRoleToAssume(function_arg, context);
    if (!has_key_pair && !has_role_to_assume)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "use_same_s3_credentials_for_base_backup requires access_key_id and secret_access_key, or extra_credentials naming a "
            "role_arn, got {}",
            toStringForLogging());

    /// What the destination may keep is what the metadata can carry back. `withoutS3Credentials` strips a
    /// positional key pair and only a source key pair can replay it, so one the source cannot reproduce
    /// would open the base backup here and fail to on restore -- it is dropped. A `role_arn` survives
    /// redaction, so a destination role is kept when the source names none, and lending credentials does
    /// not cost the destination the identity it named. A clause naming no role assumes nothing, and keeping
    /// it would only leave the locator unreconstructable from its stripped form, which is what
    /// `writeBackupMetadata` compares before emitting the marker.
    if (dest.args.size() > 1)
        dest.args.resize(1);
    if (!hasRoleToAssume(dest.function_arg, context))
        dest.function_arg = nullptr;

    if (has_key_pair)
    {
        dest.args.resize(3);
        dest.args[1] = args[1];
        dest.args[2] = args[2];
    }

    if (has_role_to_assume)
        dest.function_arg = function_arg;
}

BackupInfo BackupInfo::withoutS3Credentials(ContextPtr context) const
{
    if (backup_engine_name != "S3")
        return *this;

    BackupInfo res = *this;
    /// The resolved snapshot contains the original overrides and cannot be kept after redacting them.
    res.frozen_named_collection.reset();

    /// S3('url', 'access_key_id', 'secret_access_key') -> S3('url')
    if (res.id_arg.empty() && res.args.size() == 3)
        res.args.resize(1);

    /// S3('https://user:password@host/bucket/backup?X-Amz-Signature=...') -> S3('https://host/bucket/backup')
    if (res.id_arg.empty() && !res.args.empty() && res.args[0].getType() == Field::Types::Which::String)
        res.args[0] = removeCredentialsFromS3URL(res.args[0].safeGet<String>());

    /// S3(collection, secret_access_key = '...') -> S3(collection)
    /// The keys are the `S3` authentication arguments consumed by `registerBackupEngineS3`
    /// and `S3StorageParsedArguments::collectCredentials`, minus the non-secret role identifiers, which
    /// stay so that a role-authenticated base backup remains openable. The key is resolved with the
    /// context, so that an expression key (e.g. concat('secret_', 'access_key')) is recognized as well.
    res.kv_args.erase(
        std::remove_if(
            res.kv_args.begin(),
            res.kv_args.end(),
            [&context](const ASTPtr & kv_arg)
            {
                auto key = getEffectiveKeyValueArgName(kv_arg, context);
                return key
                    && (*key == "access_key_id" || *key == "secret_access_key" || *key == "session_token"
                        || *key == "external_id"
                        || *key == "google_adc_client_secret" || *key == "google_adc_refresh_token");
            }),
        res.kv_args.end());

    /// S3('url', extra_credentials(role_arn = '...', external_id = '...'))
    ///     -> S3('url', extra_credentials(role_arn = '...'))
    if (res.function_arg)
        res.function_arg = withoutSecretExtraCredentials(res.function_arg, context);

    /// S3(collection, url = 'https://...?X-Amz-Signature=...') -> redact the `url` override as well
    for (auto & kv_arg : res.kv_args)
    {
        auto key = getEffectiveKeyValueArgName(kv_arg, context);
        if (!key || *key != "url")
            continue;

        auto value = getKeyValueArgStringValue(kv_arg);
        bool value_is_literal = value.has_value();
        if (!value)
        {
            /// The override may be a constant expression, which `getNamedCollection` evaluates via
            /// `getParamsMapFromAST`. Evaluate it the same way, so an expression embedding credentials
            /// (e.g. url = concat('https://user:', 'password@host/bucket/backup')) is not persisted verbatim.
            if (!context)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove credentials from the non-literal `url` argument");

            ASTPtr evaluated
                = evaluateConstantExpressionOrIdentifierAsLiteral(kv_arg->as<const ASTFunction>()->arguments->children[1], context);
            const auto * literal = evaluated->as<const ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `url` argument must be a constant string expression");
            value = literal->value.safeGet<String>();
        }

        String redacted = removeCredentialsFromS3URL(*value);
        if (!value_is_literal || redacted != *value)
        {
            /// The AST may be shared with the query, so it must not be modified in place.
            ASTPtr cloned = kv_arg->clone();
            cloned->as<ASTFunction>()->arguments->children[1] = make_intrusive<ASTLiteral>(redacted);
            kv_arg = cloned;
        }
    }

    return res;
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
            /// Marked before the value is written so the mark remembers the replaced stored value.
            mutable_collection->markQueryOverridden(key);
            mutable_collection->setOrUpdate<String>(key, fieldToString(value), {});
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
