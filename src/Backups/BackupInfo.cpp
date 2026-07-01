#include <Backups/BackupInfo.h>

#include "config.h"

#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/quoteString.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#if USE_AWS_S3
#include <IO/S3/URI.h>
#endif
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/NamedCollectionsHelpers.h>
#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#endif

#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <optional>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
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
    namespace fs = std::filesystem;

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

    String toLower(String s)
    {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return s;
    }

    String trimWhitespace(const String & s)
    {
        size_t begin = 0;
        while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin])))
            ++begin;

        size_t end = s.size();
        while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1])))
            --end;

        return s.substr(begin, end - begin);
    }

    String stripTrailingSlashes(String s)
    {
        bool had_trailing_slash = !s.empty() && s.back() == '/';

        while (s.size() > 1 && s.back() == '/')
            s.pop_back();

        if (had_trailing_slash && s != "/" && hasRegisteredArchiveFileExtension(s))
            s += '/';

        return s;
    }

    String stripURLQuery(String s)
    {
        size_t query_pos = s.find_first_of("?#");
        if (query_pos != String::npos)
            s.resize(query_pos);
        return s;
    }

    String stripURLFragment(String s)
    {
        size_t fragment_pos = s.find('#');
        if (fragment_pos != String::npos)
            s.resize(fragment_pos);
        return s;
    }

    String stripURLUserInfo(String s)
    {
        size_t scheme_pos = s.find("://");
        if (scheme_pos == String::npos)
            return s;

        size_t authority_start = scheme_pos + 3;
        size_t authority_end = s.find('/', authority_start);
        if (authority_end == String::npos)
            authority_end = s.size();

        size_t userinfo_end = s.find('@', authority_start);
        if (userinfo_end != String::npos && userinfo_end < authority_end)
            s.erase(authority_start, userinfo_end - authority_start + 1);
        return s;
    }

    String getStringArgForNormalizedIdentity(const Field & arg, const String & backup_engine_name, size_t index)
    {
        if (arg.getType() != Field::Types::Which::String)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Backup engine '{}' argument {} must be a string for normalized backup identity, got {}",
                backup_engine_name,
                index + 1,
                arg.getTypeName());
        return arg.safeGet<String>();
    }

    void checkDiskNameForNormalizedIdentity(const String & disk_name, const Poco::Util::AbstractConfiguration & config)
    {
        String key = "backups.allowed_disk";
        if (!config.has(key))
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "The 'backups.allowed_disk' configuration parameter is not set, cannot use 'Disk' backup engine");

        size_t counter = 0;
        while (config.getString(key) != disk_name)
        {
            key = "backups.allowed_disk[" + std::to_string(++counter) + "]";
            if (!config.has(key))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Disk '{}' is not allowed for backups, see the 'backups.allowed_disk' configuration parameter",
                    quoteString(disk_name));
        }
    }

    fs::path checkDiskPathForNormalizedIdentity(const String & disk_name, const DiskPtr & disk, fs::path path)
    {
        path = path.lexically_normal();
        if (!path.is_relative() && disk->getDataSourceDescription().type == DataSourceType::Local)
            path = path.lexically_proximate(disk->getPath());

        bool path_ok = path.empty() || (path.is_relative() && (*path.begin() != ".."));
        if (!path_ok)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path '{}' to backup must be inside the specified disk '{}'",
                quoteString(path.c_str()),
                quoteString(disk_name));

        return path;
    }

    fs::path checkFilePathForNormalizedIdentity(
        fs::path path,
        const Poco::Util::AbstractConfiguration & config,
        const fs::path & data_dir)
    {
        path = path.lexically_normal();
        if (path.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path to backup must not be empty");

        String key = "backups.allowed_path";
        if (!config.has(key))
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "The 'backups.allowed_path' configuration parameter is not set, cannot use 'File' backup engine");

        if (path.is_relative())
        {
            fs::path first_allowed_path = config.getString(key);
            if (first_allowed_path.is_relative())
                first_allowed_path = data_dir / first_allowed_path;

            path = first_allowed_path / path;
        }

        size_t counter = 0;
        while (true)
        {
            fs::path allowed_path = config.getString(key);
            if (allowed_path.is_relative())
                allowed_path = data_dir / allowed_path;

            auto rel = path.lexically_proximate(allowed_path);
            bool path_ok = rel.empty() || (rel.is_relative() && (*rel.begin() != ".."));
            if (path_ok)
                return path;

            key = "backups.allowed_path[" + std::to_string(++counter) + "]";
            if (!config.has(key))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Path {} is not allowed for backups, see the 'backups.allowed_path' configuration parameter",
                    quoteString(path.c_str()));
        }
    }

    String normalizeS3URL(String s)
    {
#if USE_AWS_S3
        S3::URI uri(stripURLUserInfo(stripURLFragment(s)), /* allow_archive_path_syntax = */ false);
        return "endpoint=" + uri.endpoint
            + ";bucket=" + uri.bucket
            + ";key=" + stripTrailingSlashes(uri.key)
            + ";version_id=" + uri.version_id;
#else
        /// Without AWS support the `S3` backup engine cannot be used. Strip the
        /// whole query because exact presigned-parameter parsing is unavailable.
        return stripTrailingSlashes(stripURLUserInfo(stripURLQuery(s)));
#endif
    }

    String normalizeS3Path(String s)
    {
#if USE_AWS_S3
        S3::URI uri("s3://bucket/" + stripURLFragment(s), /* allow_archive_path_syntax = */ false);
        String result = stripTrailingSlashes(uri.key);
        if (!uri.version_id.empty())
        {
            result += "?versionId=";
            result += uri.version_id;
        }
        return result;
#else
        return stripTrailingSlashes(stripURLQuery(s));
#endif
    }

    String normalizeAzureConnection(String s)
    {
        if (s.find(';') == String::npos)
            return stripTrailingSlashes(stripURLQuery(s));

        std::unordered_map<String, String> parts;
        Strings redacted_parts;
        size_t start = 0;
        while (start <= s.size())
        {
            size_t end = s.find(';', start);
            String part = s.substr(start, end == String::npos ? String::npos : end - start);
            if (!part.empty())
            {
                size_t separator = part.find('=');
                String key = toLower(trimWhitespace(separator == String::npos ? part : part.substr(0, separator)));
                String value;
                if (separator != String::npos)
                {
                    value = trimWhitespace(part.substr(separator + 1));
                    parts[key] = value;
                }

                if (key != "accountkey" && key != "sharedaccesssignature" && key != "sig")
                    redacted_parts.push_back(separator == String::npos ? key : key + "=" + value);
            }

            if (end == String::npos)
                break;
            start = end + 1;
        }

        auto blob_endpoint = parts.find("blobendpoint");
        if (blob_endpoint != parts.end())
            return stripTrailingSlashes(stripURLQuery(blob_endpoint->second));

        auto protocol = parts.find("defaultendpointsprotocol");
        auto account_name = parts.find("accountname");
        auto endpoint_suffix = parts.find("endpointsuffix");
        if (account_name != parts.end() && endpoint_suffix != parts.end())
        {
            String result = protocol == parts.end() ? "https" : protocol->second;
            result += "://";
            result += account_name->second;
            result += ".blob.";
            result += endpoint_suffix->second;
            return stripTrailingSlashes(result);
        }

        std::sort(redacted_parts.begin(), redacted_parts.end());

        String result;
        for (const auto & part : redacted_parts)
        {
            if (!result.empty())
                result += ';';
            result += part;
        }
        return result;
    }

    bool isCredentialArgForNormalizedIdentity(const String & backup_engine_name, bool has_named_collection, size_t index)
    {
        if (backup_engine_name == "S3")
            return index == 1 || index == 2;
        if (backup_engine_name == "AzureBlobStorage")
            return has_named_collection ? (index == 1 || index == 2) : (index == 3 || index == 4);
        return false;
    }

    bool isCredentialKeyForNormalizedIdentity(const String & backup_engine_name, const String & key)
    {
        String lower_key = toLower(key);
        if (backup_engine_name == "S3")
            return lower_key == "access_key_id"
                || lower_key == "secret_access_key"
                || lower_key == "session_token"
                || lower_key == "use_environment_credentials"
                || lower_key == "no_sign_request"
                || lower_key == "expiration_window_seconds"
                || lower_key == "role_arn"
                || lower_key == "role_session_name"
                || lower_key == "http_client"
                || lower_key == "service_account"
                || lower_key == "metadata_service"
                || lower_key == "external_id"
                || lower_key == "request_token_path";

        if (backup_engine_name == "AzureBlobStorage")
            return lower_key == "account_name"
                || lower_key == "account_key"
                || lower_key == "client_id"
                || lower_key == "tenant_id"
                || lower_key == "shared_access_signature";

        return false;
    }

    String normalizeArgForIdentity(const String & backup_engine_name, bool has_named_collection, size_t index, String arg)
    {
        if (backup_engine_name == "S3" && has_named_collection && index == 0)
            return normalizeS3Path(arg);
        if (backup_engine_name == "S3" && !has_named_collection && index == 0)
            return normalizeS3URL(arg);
        if (backup_engine_name == "AzureBlobStorage" && !has_named_collection && index == 0)
            return normalizeAzureConnection(arg);
        if (backup_engine_name == "Disk" && index == 1)
            return stripTrailingSlashes(fs::path(arg).lexically_normal().string());
        if (backup_engine_name == "File" && index == 0)
            return stripTrailingSlashes(fs::path(arg).lexically_normal().string());
        return stripTrailingSlashes(arg);
    }

    String normalizeKeyValueArgForIdentity(const String & backup_engine_name, const String & key, const String & value)
    {
        String lower_key = toLower(key);
        if (backup_engine_name == "S3" && lower_key == "url")
            return normalizeS3URL(value);
        if (backup_engine_name == "S3" && lower_key == "filename")
            return normalizeS3Path(value);
        if (backup_engine_name == "AzureBlobStorage" && (lower_key == "connection_string" || lower_key == "storage_account_url"))
            return normalizeAzureConnection(value);
        return stripTrailingSlashes(value);
    }

    void appendNormalizedIdentityComponent(String & result, bool & first, const String & component)
    {
        if (!first)
            result += ',';
        first = false;
        result += std::to_string(component.size());
        result += ':';
        result += component;
    }

    String appendPath(String base, const String & path)
    {
        if (path.empty())
            return base;
        if (base.empty() || base.back() == '/')
            return base + path;
        return base + "/" + path;
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

String BackupInfo::toNormalizedString() const
{
    String result = backup_engine_name + "(";
    bool first = true;
    const bool has_named_collection = !id_arg.empty();

    if (has_named_collection)
        appendNormalizedIdentityComponent(result, first, id_arg);

    for (size_t i = 0; i < args.size(); ++i)
    {
        if (isCredentialArgForNormalizedIdentity(backup_engine_name, has_named_collection, i))
            continue;

        appendNormalizedIdentityComponent(
            result,
            first,
            normalizeArgForIdentity(
                backup_engine_name,
                has_named_collection,
                i,
                getStringArgForNormalizedIdentity(args[i], backup_engine_name, i)));
    }

    /// Direct backup engines ignore key-value arguments. Include overrides only
    /// with named collections, where they can change the effective destination.
    if (has_named_collection && !kv_args.empty())
    {
        Strings kv_strings;
        kv_strings.reserve(kv_args.size());
        for (const auto & kv : kv_args)
        {
            auto key = getKeyValueArgName(kv);
            if (!key)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Backup engine '{}' key-value argument {} must have a string key for normalized backup identity",
                    backup_engine_name,
                    kv->formatForErrorMessage());

            if (isCredentialKeyForNormalizedIdentity(backup_engine_name, *key))
                continue;

            auto value = getKeyValueArgStringValue(kv);
            if (!value)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Backup engine '{}' key-value argument {} must have a string value for normalized backup identity",
                    backup_engine_name,
                    kv->formatForErrorMessage());

            String lower_key = toLower(*key);
            kv_strings.push_back(
                lower_key + "=" + normalizeKeyValueArgForIdentity(backup_engine_name, lower_key, *value));
        }

        std::sort(kv_strings.begin(), kv_strings.end());
        for (const auto & kv_str : kv_strings)
            appendNormalizedIdentityComponent(result, first, kv_str);
    }

    result += ')';
    return result;
}

String BackupInfo::toNormalizedString(ContextPtr context) const
{
    auto normalize_with_context = [&](BackupInfo info)
    {
        if (!context)
            return info.toNormalizedString();

        if (info.backup_engine_name == "Disk" && info.args.size() == 2)
        {
            const String disk_name = getStringArgForNormalizedIdentity(info.args[0], info.backup_engine_name, 0);
            checkDiskNameForNormalizedIdentity(disk_name, context->getConfigRef());
            auto disk = context->getDisk(disk_name);
            fs::path path = getStringArgForNormalizedIdentity(info.args[1], info.backup_engine_name, 1);
            info.args[1] = checkDiskPathForNormalizedIdentity(disk_name, disk, path).string();
            return info.toNormalizedString();
        }

        if (info.backup_engine_name == "File" && info.args.size() == 1)
        {
            fs::path path = getStringArgForNormalizedIdentity(info.args[0], info.backup_engine_name, 0);
            info.args[0] = checkFilePathForNormalizedIdentity(path, context->getConfigRef(), context->getPath()).string();
            return info.toNormalizedString();
        }

#if USE_AZURE_BLOB_STORAGE
        if (info.backup_engine_name == "AzureBlobStorage" && (info.args.size() == 3 || info.args.size() == 5))
        {
            const String connection_url = getStringArgForNormalizedIdentity(info.args[0], info.backup_engine_name, 0);
            const String container_name = getStringArgForNormalizedIdentity(info.args[1], info.backup_engine_name, 1);
            const String blob_path = getStringArgForNormalizedIdentity(info.args[2], info.backup_engine_name, 2);
            std::optional<String> account_name;
            std::optional<String> account_key;
            if (info.args.size() == 5)
            {
                account_name = getStringArgForNormalizedIdentity(info.args[3], info.backup_engine_name, 3);
                account_key = getStringArgForNormalizedIdentity(info.args[4], info.backup_engine_name, 4);
            }

            auto connection_params = getAzureConnectionParams(
                connection_url,
                container_name,
                account_name,
                account_key,
                std::nullopt,
                std::nullopt,
                context);

            info.args.clear();
            info.args.emplace_back(connection_params.getConnectionURL());
            info.args.emplace_back(connection_params.getContainer());
            info.args.emplace_back(blob_path);
            return info.toNormalizedString();
        }
#endif

        return info.toNormalizedString();
    };

    if (id_arg.empty())
        return normalize_with_context(*this);

    auto collection = getNamedCollection(context);
    BackupInfo resolved = *this;
    resolved.id_arg.clear();
    resolved.kv_args.clear();
    resolved.args.clear();

    if (backup_engine_name == "S3")
    {
        String url = collection->get<String>("url");
        if (collection->has("filename"))
            url = appendPath(url, collection->get<String>("filename"));
        if (!args.empty())
            url = appendPath(url, getStringArgForNormalizedIdentity(args[0], backup_engine_name, 0));

        resolved.args.emplace_back(url);
        return normalize_with_context(resolved);
    }

    if (backup_engine_name == "AzureBlobStorage")
    {
        String connection_url = collection->getAnyOrDefault<String>({"connection_string", "storage_account_url"}, "");
        String container_name = collection->get<String>("container");
        String blob_path = collection->getOrDefault<String>("blob_path", "");
        if (!args.empty())
            blob_path = getStringArgForNormalizedIdentity(args[0], backup_engine_name, 0);

        resolved.args.emplace_back(connection_url);
        resolved.args.emplace_back(container_name);
        resolved.args.emplace_back(blob_path);
        return normalize_with_context(resolved);
    }

    return normalize_with_context(*this);
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

    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Context is required to resolve named collection `{}`", id_arg);

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
        for (const auto & [key, value] : params_from_query)
            mutable_collection->setOrUpdate<String>(key, fieldToString(value), {});
        collection = std::move(mutable_collection);
    }

    return collection;
}

BackupInfo BackupInfo::freezeNamedCollection(ContextPtr context) const
{
    if (id_arg.empty())
        return *this;

    BackupInfo res = *this;
    res.frozen_named_collection = getNamedCollection(context)->duplicate();
    return res;
}

}
