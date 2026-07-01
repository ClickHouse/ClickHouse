#include <Backups/BackupInfo.h>

#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Poco/URI.h>

#include <algorithm>
#include <optional>


namespace DB
{
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

    /// Removes the credentials embedded in the URL itself: the userinfo part and the authentication
    /// query parameters of presigned URLs (the same set as recognized by `S3::URI`).
    /// Destination-significant parameters such as `versionId` are kept.
    /// Returns the URL unchanged if there is nothing to remove.
    String removeCredentialsFromS3URL(const String & url)
    {
        if (url.find('@') == String::npos && url.find('?') == String::npos)
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

bool BackupInfo::canCopyS3CredentialsTo(const BackupInfo & dest) const
{
    /// Must mirror the conditions checked by `copyS3CredentialsTo`.
    return id_arg.empty() && dest.id_arg.empty()
        && backup_engine_name == "S3" && dest.backup_engine_name == "S3"
        && args.size() == 3;
}

void BackupInfo::copyS3CredentialsTo(BackupInfo & dest) const
{
    /// named_collection case, no need to update
    if (!dest.id_arg.empty() || !id_arg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup is not compatible with named_collections");

    if (backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", toStringForLogging());
    if (dest.backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", dest.toStringForLogging());
    if (args.size() != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup requires access_key_id, secret_access_key, got {}", toStringForLogging());

    auto & dest_args = dest.args;
    dest_args.resize(3);
    dest_args[1] = args[1];
    dest_args[2] = args[2];
}

BackupInfo BackupInfo::withoutS3Credentials(ContextPtr context) const
{
    if (backup_engine_name != "S3")
        return *this;

    BackupInfo res = *this;

    /// S3('url', 'access_key_id', 'secret_access_key') -> S3('url')
    if (res.id_arg.empty() && res.args.size() == 3)
        res.args.resize(1);

    /// S3('https://user:password@host/bucket/backup?X-Amz-Signature=...') -> S3('https://host/bucket/backup')
    if (res.id_arg.empty() && !res.args.empty() && res.args[0].getType() == Field::Types::Which::String)
        res.args[0] = removeCredentialsFromS3URL(res.args[0].safeGet<String>());

    /// S3(collection, secret_access_key = '...') -> S3(collection)
    /// The keys are the `S3` authentication arguments consumed by `registerBackupEngineS3`
    /// and `S3StorageParsedArguments::collectCredentials`. The key is resolved with the context, so
    /// that an expression key (e.g. concat('secret_', 'access_key')) is recognized as well.
    res.kv_args.erase(
        std::remove_if(
            res.kv_args.begin(),
            res.kv_args.end(),
            [&context](const ASTPtr & kv_arg)
            {
                auto key = getEffectiveKeyValueArgName(kv_arg, context);
                return key
                    && (*key == "access_key_id" || *key == "secret_access_key" || *key == "session_token" || *key == "role_arn"
                        || *key == "role_session_name" || *key == "external_id");
            }),
        res.kv_args.end());

    /// S3('url', extra_credentials(role_arn = '...')) -> S3('url')
    res.function_arg = nullptr;

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
        for (const auto & [key, value] : params_from_query)
            mutable_collection->setOrUpdate<String>(key, fieldToString(value), {});
        collection = std::move(mutable_collection);
    }

    return collection;
}

}
