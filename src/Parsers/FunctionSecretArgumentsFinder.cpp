#include <Parsers/FunctionSecretArgumentsFinder.h>

#include <algorithm>

#include <Common/KnownObjectNames.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/maskURIPassword.h>
#include <Core/QualifiedTableName.h>
#include <base/defines.h>

namespace DB
{

namespace
{
    /// Masks credential material embedded in an S3 URL itself: the userinfo part and the values of
    /// presigned-URL query parameters. The parameter set mirrors `BackupInfo::removeCredentialsFromS3URL`
    /// (which strips the same fields from persisted backup metadata). Returns true if anything was masked.
    bool maskS3URICredentials(String & url)
    {
        /// Both scans live in `Common/maskURIPassword.h` and are checked against the regular
        /// expressions they replaced in `src/Common/tests/gtest_mask_uri_password.cpp`.
        bool changed = maskURIUserinfo(url);
        changed |= maskPresignedURLParameters(url);
        return changed;
    }
}

void FunctionSecretArgumentsFinder::markSecretArgument(size_t index, bool argument_is_named)
{
    if (index >= function->arguments->size())
        return;
    chassert(result.replacement.empty()); /// We shouldn't use replacement with masking other arguments
    /// Each argument is masked individually: valid S3 syntax can interleave secrets with non-secret
    /// arguments, which a contiguous span cannot represent without hiding the arguments in between.
    /// A malformed query can mark the same index as both named and positional; the positional form
    /// wins, hiding the argument whole (fail closed).
    auto [it, inserted] = result.masked_arguments.emplace(index, argument_is_named);
    if (!inserted)
        it->second &= argument_is_named;
}

void FunctionSecretArgumentsFinder::maskNestedSecretMaps()
{
    for (size_t i = 0, size = function->arguments->size(); i < size; ++i)
    {
        const auto f = function->arguments->at(i)->getFunction();
        if (!f)
            continue;
        const auto name = f->name();
        if ((name == "headers" || name == "extra_credentials")
            && std::find(result.nested_maps.begin(), result.nested_maps.end(), name) == result.nested_maps.end())
            result.nested_maps.push_back(name);
    }
}

std::vector<size_t> FunctionSecretArgumentsFinder::classifyS3Arguments(size_t start, bool positionals_allowed_after_named)
{
    maskNestedSecretMaps();

    std::vector<size_t> positional;
    bool seen_named = false;
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        if (const auto f = function->arguments->at(i)->getFunction())
        {
            const auto name = f->name();
            if (name == "headers" || name == "extra_credentials")
                continue;
            if (name == "equals" && f->hasArguments() && f->arguments->size() == 2)
            {
                seen_named = true;
                String key;
                if (f->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
                {
                    if (std::find(std::begin(s3_secret_keys), std::end(s3_secret_keys), key) != std::end(s3_secret_keys))
                    {
                        markSecretArgument(i, /* argument_is_named= */ true);
                    }
                    else if (key == "url")
                    {
                        /// A `url` override can itself carry credentials (userinfo, presign parameters).
                        String url;
                        if (f->arguments->at(1)->tryGetString(&url, /* allow_identifier= */ false))
                        {
                            if (maskS3URICredentials(url))
                                result.replaced_arguments[i] = "url = " + quoteString(url);
                        }
                        else
                        {
                            /// A url built from an expression can embed credentials in its pieces;
                            /// we cannot evaluate it here, so fail closed and hide the value.
                            markSecretArgument(i, /* argument_is_named= */ true);
                        }
                    }
                    else if (!f->arguments->at(1)->tryGetString(nullptr, /* allow_identifier= */ true)
                             && !f->arguments->at(1)->tryGetLiteralText(nullptr))
                    {
                        /// A visible non-secret override (`format`, `structure`, `role_arn`, ...) whose
                        /// value is not a plain literal or identifier can be a nested secret carrier,
                        /// e.g. `format = headers('Authorization' = '...')`, formatted verbatim before
                        /// the parser rejects the non-literal value. Fail closed and hide the value.
                        markSecretArgument(i, /* argument_is_named= */ true);
                    }
                }
                else
                {
                    /// The parsers evaluate the key as a constant expression, so it can name any secret
                    /// key. We cannot evaluate it here, so fail closed and hide the value (the key
                    /// expression itself stays visible; keys are not secrets).
                    markSecretArgument(i, /* argument_is_named= */ true);
                }
                continue;
            }
        }
        if (seen_named && !positionals_allowed_after_named)
        {
            /// The parsers reject positional arguments after the first `key = value` argument, but the
            /// query is logged before validation and the intended slot is unknowable; fail closed.
            markSecretArgument(i);
            continue;
        }
        positional.push_back(i);
    }
    return positional;
}

void FunctionSecretArgumentsFinder::maskS3PositionalSecrets(
    const std::vector<size_t> & positional, size_t url_slot, bool with_structure)
{
    /// The parser (`S3StorageParsedArguments::fromAST`) selects the signature from the positional
    /// `count` (the number of arguments from `url` on) and `with_structure`, disambiguating only
    /// NOSIGN and format-vs-secret by looking at an argument's value. Across every signature the only
    /// credential positionals are `secret_access_key` at slot 2 and `session_token` at slot 3, so we
    /// reproduce the parser's per-count decision for just those two slots.
    ///
    /// Value tests fail closed: an unevaluable expression is not recognized as NOSIGN or a format, so
    /// the slot that would then be a credential is masked. A query built from a computed format thus
    /// loses that non-secret argument in the AST dump, which is safe. The query-tree path resolves such
    /// expressions to constants first, so it classifies them exactly.
    if (url_slot >= positional.size())
        return;
    const size_t count = positional.size() - url_slot;

    auto value_is = [&](size_t slot, auto && predicate) -> bool
    {
        String value;
        return url_slot + slot < positional.size()
            && tryGetStringFromArgument(positional[url_slot + slot], &value) && predicate(value);
    };
    auto is_nosign = [&](size_t slot) { return value_is(slot, [](const String & v) { return equalsCaseInsensitive(v, "NOSIGN"); }); };
    auto is_format = [&](size_t slot) { return value_is(slot, [](const String & v) { return v == "auto" || KnownFormatNames::instance().exists(v); }); };

    bool secret_access_key = false; /// slot 2
    bool session_token = false;     /// slot 3
    switch (count)
    {
        case 0: case 1: case 2: /// url only, or url + format/NOSIGN
            break;
        case 3:
            secret_access_key = !is_nosign(1) && !is_format(1);
            break;
        case 4:
            secret_access_key = !is_nosign(1) && !(with_structure && is_format(1));
            session_token = secret_access_key && !is_format(3);
            break;
        case 5:
            secret_access_key = !with_structure || !is_nosign(1);
            session_token = secret_access_key && !is_format(3);
            break;
        case 6:
            secret_access_key = true;
            session_token = !with_structure || !is_format(3);
            break;
        default: /// count >= 7: access-key form only, both credential slots always present
            secret_access_key = true;
            session_token = true;
            break;
    }

    if (secret_access_key)
        markSecretArgument(positional[url_slot + 2]);
    if (session_token)
        markSecretArgument(positional[url_slot + 3]);
}

void FunctionSecretArgumentsFinder::maskS3PositionalsFrom(const std::vector<size_t> & positional, size_t first_slot)
{
    for (size_t slot = first_slot; slot < positional.size(); ++slot)
        markSecretArgument(positional[slot]);
}

void FunctionSecretArgumentsFinder::maskS3UrlArgument(const std::vector<size_t> & positional, size_t url_slot)
{
    if (url_slot >= positional.size())
        return;
    String url;
    if (!tryGetStringFromArgument(positional[url_slot], &url, /* allow_identifier= */ false))
    {
        /// The parsers evaluate a constant-expression url before signature parsing, so a url built
        /// from an expression can embed credentials in its pieces; we cannot evaluate it here, so
        /// fail closed and hide it whole.
        markSecretArgument(positional[url_slot]);
        return;
    }
    if (maskS3URICredentials(url))
        result.replaced_arguments[positional[url_slot]] = quoteString(url);
}

void FunctionSecretArgumentsFinder::findOrdinaryFunctionSecretArguments()
{
    if ((function->name() == "mysql") || (function->name() == "postgresql"))
    {
        /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
        /// postgresql('host:port', 'database', 'table', 'user', 'password', ...)
        /// mongodb('host:port', 'database', 'collection', 'user', 'password', ...)
        findMySQLFunctionSecretArguments();
    }
    else if (function->name() == "mongodb")
    {
        findMongoDBSecretArguments();
    }
    else if ((function->name() == "s3") || (function->name() == "cosn") || (function->name() == "oss") ||
             (function->name() == "deltaLake") || (function->name() == "deltaLakeS3") || (function->name() == "hudi") ||
             (function->name() == "iceberg") || (function->name() == "gcs") || (function->name() == "icebergS3") ||
             (function->name() == "paimon") || (function->name() == "paimonS3"))
    {
        /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
        findS3FunctionSecretArguments(/* is_cluster_function= */ false);
    }
    else if ((function->name() == "s3Cluster") || (function ->name() == "hudiCluster") ||
             (function ->name() == "deltaLakeCluster") || (function ->name() == "deltaLakeS3Cluster") ||
             (function ->name() == "icebergS3Cluster") || (function ->name() == "icebergCluster") ||
             (function ->name() == "paimonCluster") || (function ->name() == "paimonS3Cluster"))
    {
        /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
        findS3FunctionSecretArguments(/* is_cluster_function= */ true);
    }
    else if ((function->name() == "azureBlobStorage") || (function->name() == "deltaLakeAzure") ||
             (function->name() == "icebergAzure") || (function->name() == "paimonAzure"))
    {
        /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure)
        findAzureBlobStorageFunctionSecretArguments(/* is_cluster_function= */ false);
    }
    else if ((function->name() == "azureBlobStorageCluster") || (function->name() == "icebergAzureCluster") ||
             (function->name() == "deltaLakeAzureCluster") || (function->name() == "paimonAzureCluster"))
    {
        /// azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
        findAzureBlobStorageFunctionSecretArguments(/* is_cluster_function= */ true);
    }
    else if ((function->name() == "remote") || (function->name() == "remoteSecure"))
    {
        /// remote('addresses_expr', 'db', 'table', 'user', 'password', ...)
        findRemoteFunctionSecretArguments();
    }
    else if ((function->name() == "encrypt") || (function->name() == "decrypt") ||
                (function->name() == "aes_encrypt_mysql") || (function->name() == "aes_decrypt_mysql") ||
                (function->name() == "tryDecrypt"))
    {
        /// encrypt('mode', 'plaintext', 'key' [, iv, aad])
        findEncryptionFunctionSecretArguments();
    }
    else if (equalsCaseInsensitive(function->name(), "HMAC"))
    {
        /// HMAC('mode', 'message', 'key') -> HMAC('mode', 'message', '[HIDDEN]')
        findHMACSecretArguments();
    }
    else if (function->name() == "url" || function->name() == "urlCluster")
    {
        /// url('url', ...) keeps the url at slot 0; urlCluster('cluster', 'url', ...) at slot 1.
        findURLSecretArguments(function->name() == "urlCluster" ? 1 : 0);
    }
    else if (function->name() == "redis")
    {
        findRedisFunctionSecretArguments();
    }
    else if (function->name() == "ytsaurus")
    {
        findYTsaurusStorageTableEngineSecretArguments();
    }
    else if ((function->name() == "arrowFlight") || (function->name() == "arrowflight"))
    {
        findArrowFlightSecretArguments();
    }
    else if ((function->name() == "jdbc") || (function->name() == "odbc"))
    {
        /// jdbc('DSN', schema, table) or jdbc('DSN', table)
        /// odbc('DSN', schema, table) or odbc('DSN', table)
        /// The DSN (connection string) may contain credentials.
        findXDBCSecretArguments();
    }
}

void FunctionSecretArgumentsFinder::findMySQLFunctionSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// mysql(named_collection, ..., password = 'password', ...)
        findSecretNamedArgument("password", 1);
    }
    else
    {
        /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
        markSecretArgument(4);
    }
}

void FunctionSecretArgumentsFinder::findMongoDBSecretArguments()
{
    String uri;

    if (isNamedCollectionName(0))
    {
        /// MongoDB(named_collection, ..., password = 'password', ...)
        if (findSecretNamedArgument("password", 1))
            return;

        /// MongoDB(named_collection, ..., uri = 'mongodb://username:password@127.0.0.1:27017', ...)
        if (findNamedArgument(&uri, "uri", 1) == -1)
            return;

        result.are_named = true;
        result.start = 1;
    }
    else if (function->arguments->size() == 2)
    {
        tryGetStringFromArgument(0, &uri);
        result.are_named = false;
        result.start = 0;
    }
    else
    {
        // MongoDB('127.0.0.1:27017', 'database', 'collection', 'user, 'password'...)
        markSecretArgument(4, false);
        return;
    }

    chassert(result.count == 0);
    maskURIPassword(&uri);
    result.count = 1;
    result.replacement = std::move(uri);
}

void FunctionSecretArgumentsFinder::findRedisTableEngineSecretArguments()
{
    /// Redis does not have URL/address argument,
    /// only 'host:port' and separate "password" argument.

    if (isNamedCollectionName(0))
    {
        if (findSecretNamedArgument("password", 1))
            return;
    }
    else
    {
        // Redis('host:port', 'db_index', 'password', 'pool_size')
        markSecretArgument(2, false);
        return;
    }
}

void FunctionSecretArgumentsFinder::findArrowFlightSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// ArrowFlight(named_collection, ..., password = 'password')
        findSecretNamedArgument("password", 1);
    }
    else
    {
        /// ArrowFlight('host:port', 'dataset', 'username', 'password')
        markSecretArgument(3);
    }
}

void FunctionSecretArgumentsFinder::findXDBCSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// jdbc(named_collection, ..., datasource = 'DSN', ...)
        /// odbc(named_collection, ..., connection_settings = 'DSN', ...)
        /// `datasource` and `connection_settings` are mutually exclusive aliases.
        /// If the value is a URI, mask only the password; otherwise hide the whole value.
        /// If somehow both are present (invalid query), hide all named arguments.
        ssize_t ds_idx = findNamedArgument(nullptr, "datasource", 1);
        ssize_t cs_idx = findNamedArgument(nullptr, "connection_settings", 1);

        if (ds_idx >= 0 && cs_idx >= 0)
        {
            /// Both present — hide all named arguments starting from index 1.
            result.start = 1;
            result.count = function->arguments->size() - 1;
            result.are_named = true;
        }
        else if (ds_idx >= 0)
            maskXDBCSecretNamedArgument("datasource", 1);
        else if (cs_idx >= 0)
            maskXDBCSecretNamedArgument("connection_settings", 1);
    }
    else
    {
        /// jdbc('DSN', schema, table) / jdbc('DSN', table)
        /// odbc('DSN', schema, table) / odbc('DSN', table)
        /// JDBC('DSN', database, table) / ODBC('DSN', database, table)
        /// The connection string may be a URI with credentials embedded,
        /// e.g. scheme://username:password@host:port/dbname
        /// If so, mask only the password part; otherwise hide the whole argument.
        String uri;
        if (tryGetStringFromArgument(0, &uri))
        {
            if (maskURIPassword(&uri))
            {
                chassert(result.count == 0);
                result.start = 0;
                result.count = 1;
                result.replacement = std::move(uri);
                return;
            }
        }
        markSecretArgument(0, false);
    }
}

void FunctionSecretArgumentsFinder::maskXDBCSecretNamedArgument(std::string_view key, size_t start)
{
    String value;
    ssize_t arg_idx = findNamedArgument(&value, key, start);
    if (arg_idx < 0)
        return;

    if (!value.empty() && maskURIPassword(&value))
    {
        result.are_named = true;
        result.start = arg_idx;
        result.count = 1;
        result.replacement = std::move(value);
    }
    else
    {
        markSecretArgument(arg_idx, /* argument_is_named= */ true);
    }
}

void FunctionSecretArgumentsFinder::findS3FunctionSecretArguments(bool is_cluster_function)
{
    /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
    size_t url_slot = is_cluster_function ? 1 : 0;

    if (isNamedCollectionName(url_slot))
    {
        /// s3(named_collection, ..., secret_access_key = 'secret_access_key', ...)
        /// s3Cluster('cluster_name', named_collection, ..., secret_access_key = 'secret_access_key', ...)
        findS3NamedCollectionSecretArguments(url_slot + 1);
        return;
    }

    const auto positional = classifyS3Arguments();
    maskS3UrlArgument(positional, url_slot);

    /// The table function accepts a positional `structure`, unless a `structure = ...` named override
    /// is given (the parser then turns `with_structure` off). The parser evaluates key expressions, so
    /// an unevaluable key might resolve to `structure`; treat any unreadable key as disabling it too.
    /// This fails closed: `with_structure = false` only ever masks the same slots or more.
    bool with_structure = true;
    for (size_t i = 0; i < function->arguments->size(); ++i)
    {
        const auto equals_func = function->arguments->at(i)->getFunction();
        if (!equals_func || equals_func->name() != "equals" || !equals_func->hasArguments() || equals_func->arguments->size() != 2)
            continue;
        String key;
        if (!equals_func->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true) || key == "structure")
        {
            with_structure = false;
            break;
        }
    }
    maskS3PositionalSecrets(positional, url_slot, with_structure);
}

void FunctionSecretArgumentsFinder::findAzureBlobStorageFunctionSecretArguments(bool is_cluster_function)
{
    /// azureBlobStorageCluster('cluster_name', 'conn_string/storage_account_url', ...) has 'conn_string/storage_account_url' as its second argument.
    size_t url_arg_idx = is_cluster_function ? 1 : 0;

    if (!is_cluster_function && isNamedCollectionName(0))
    {
        /// azureBlobStorage(named_collection, ..., account_key = 'account_key', ...)
        if (maskAzureConnectionString(-1, true, 1))
            return;
        findSecretNamedArgument("account_key", 1);
        return;
    }
    if (is_cluster_function && isNamedCollectionName(1))
    {
        /// azureBlobStorageCluster(cluster, named_collection, ..., account_key = 'account_key', ...)
        if (maskAzureConnectionString(-1, true, 2))
            return;
        findSecretNamedArgument("account_key", 2);
        return;
    }

    if (maskAzureConnectionString(url_arg_idx))
        return;

    /// We should check other arguments first because we don't need to do any replacement in case of
    /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    /// azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    size_t count = function->arguments->size();
    if ((url_arg_idx + 4 <= count) && (count <= url_arg_idx + 7))
    {
        String fourth_arg;
        if (tryGetStringFromArgument(url_arg_idx + 3, &fourth_arg))
        {
            if (fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg))
                return;
        }
    }

    /// We're going to replace 'account_key' with '[HIDDEN]' if account_key is used in the signature
    if (url_arg_idx + 4 < count)
        markSecretArgument(url_arg_idx + 4);
}

bool FunctionSecretArgumentsFinder::maskAzureConnectionString(ssize_t url_arg_idx, bool argument_is_named, size_t start)
{
    String url_arg;
    if (argument_is_named)
    {
        url_arg_idx = findNamedArgument(&url_arg, "connection_string", start);
        if (url_arg_idx == -1 || url_arg.empty())
            url_arg_idx = findNamedArgument(&url_arg, "storage_account_url", start);
        if (url_arg_idx == -1 || url_arg.empty())
            return false;
    }
    else
    {
        if (!tryGetStringFromArgument(url_arg_idx, &url_arg))
            return false;
    }

    if (!url_arg.starts_with("http"))
    {
        if (maskConnectionStringKey(url_arg, "AccountKey="))
        {
            chassert(result.count == 0); /// We shouldn't use replacement with masking other arguments
            result.start = url_arg_idx;
            result.are_named = argument_is_named;
            result.count = 1;
            result.replacement = url_arg;
            return true;
        }

        if (maskConnectionStringKey(url_arg, "SharedAccessSignature="))
        {
            chassert(result.count == 0); /// We shouldn't use replacement with masking other arguments
            result.start = url_arg_idx;
            result.are_named = argument_is_named;
            result.count = 1;
            result.replacement = url_arg;
            return true;
        }
    }

    return false;
}

void FunctionSecretArgumentsFinder::findURLSecretArguments(size_t url_offset)
{
    /// `headers(...)` can appear at any position in every url form (function, cluster function, engine,
    /// and the named-collection variant); mask its values regardless of the url offset or a leading
    /// collection/cluster argument.
    maskNestedSecretMaps();

    if (isNamedCollectionName(url_offset))
    {
        /// url(named_collection, url = 'https://user:password@host/...', headers(...), ...): mask the
        /// userinfo password of a `url` override. The parser evaluates constant-expression keys and
        /// values, so fail closed on anything we cannot read as a plain literal (a nested `headers(...)`
        /// map or other expression could carry a secret): an unevaluable key can name `url`, and any
        /// non-literal value of a visible override can hide a nested secret. The headers are handled
        /// above; a `key = value` override is the only other shape here.
        for (size_t i = url_offset + 1; i < function->arguments->size(); ++i)
        {
            const auto equals_func = function->arguments->at(i)->getFunction();
            if (!equals_func || equals_func->name() != "equals" || !equals_func->hasArguments()
                || equals_func->arguments->size() != 2)
                continue;

            String key;
            if (!equals_func->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
            {
                markSecretArgument(i, /* argument_is_named= */ true);
            }
            else if (key == "url")
            {
                String url;
                if (equals_func->arguments->at(1)->tryGetString(&url, /* allow_identifier= */ false))
                {
                    if (maskURIPassword(&url))
                        result.replaced_arguments[i] = "url = " + quoteString(url);
                }
                else
                    markSecretArgument(i, /* argument_is_named= */ true);
            }
            else if (!equals_func->arguments->at(1)->tryGetString(nullptr, /* allow_identifier= */ true)
                     && !equals_func->arguments->at(1)->tryGetLiteralText(nullptr))
            {
                markSecretArgument(i, /* argument_is_named= */ true);
            }
        }
        return;
    }

    String uri;
    if (tryGetStringFromArgument(url_offset, &uri, /* allow_identifier= */ false))
    {
        /// A readable url literal: mask only its userinfo password, keeping the host and path visible.
        if (maskURIPassword(&uri))
            result.replaced_arguments[url_offset] = quoteString(uri);
    }
    else
    {
        /// A url built from a constant expression can embed credentials in its pieces, which we cannot
        /// evaluate here; hide it whole rather than leak (fail closed).
        markSecretArgument(url_offset);
    }
}

bool FunctionSecretArgumentsFinder::tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier) const
{
    if (arg_idx >= function->arguments->size())
        return false;

    return tryGetStringFromArgument(*function->arguments->at(arg_idx), res, allow_identifier);
}

bool FunctionSecretArgumentsFinder::tryGetStringFromArgument(const AbstractFunction::Argument & argument, String * res, bool allow_identifier)
{
    return argument.tryGetString(res, allow_identifier);
}

void FunctionSecretArgumentsFinder::findRemoteFunctionSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// remote(named_collection, ..., password = 'password', ...)
        findSecretNamedArgument("password", 1);
        return;
    }

    /// We're going to replace 'password' with '[HIDDEN'] for the following signatures:
    /// remote('addresses_expr', db.table, 'user' [, 'password'] [, sharding_key])
    /// remote('addresses_expr', 'db', 'table', 'user' [, 'password'] [, sharding_key])
    /// remote('addresses_expr', table_function(), 'user' [, 'password'] [, sharding_key])

    /// But we should check the number of arguments first because we don't need to do any replacements in case of
    /// remote('addresses_expr', db.table)
    if (function->arguments->size() < 3)
        return;

    size_t arg_num = 1;

    /// Skip 1 or 2 arguments with table_function() or db.table or 'db', 'table'.
    auto table_function = function->arguments->at(arg_num)->getFunction();
    if (table_function && KnownTableFunctionNames::instance().exists(table_function->name()))
    {
        ++arg_num;
    }
    else
    {
        std::optional<String> database;
        std::optional<QualifiedTableName> qualified_table_name;
        if (!tryGetDatabaseNameOrQualifiedTableName(arg_num, database, qualified_table_name))
        {
            /// We couldn't evaluate the argument so we don't know whether it is 'db.table' or just 'db'.
            /// Hence we can't figure out whether we should skip one argument 'user' or two arguments 'table', 'user'
            /// before the argument 'password'. So it's safer to wipe two arguments just in case.
            /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
            /// before wiping it (because the `password` argument is always a literal string).
            if (tryGetStringFromArgument(arg_num + 2, nullptr, /* allow_identifier= */ false))
            {
                /// Wipe either `password` or `user`.
                markSecretArgument(arg_num + 2);
            }
            if (tryGetStringFromArgument(arg_num + 3, nullptr, /* allow_identifier= */ false))
            {
                /// Wipe either `password` or `sharding_key`.
                markSecretArgument(arg_num + 3);
            }
            return;
        }

        /// Skip the current argument (which is either a database name or a qualified table name).
        ++arg_num;
        if (database)
        {
            /// Skip the 'table' argument if the previous argument was a database name.
            ++arg_num;
        }
    }

    /// Skip username.
    ++arg_num;

    /// Do our replacement:
    /// remote('addresses_expr', db.table, 'user', 'password', ...) -> remote('addresses_expr', db.table, 'user', '[HIDDEN]', ...)
    /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
    /// before wiping it (because the `password` argument is always a literal string).
    bool can_be_password = tryGetStringFromArgument(arg_num, nullptr, /* allow_identifier= */ false);
    if (can_be_password)
        markSecretArgument(arg_num);
}

bool FunctionSecretArgumentsFinder::tryGetDatabaseNameOrQualifiedTableName(
    size_t arg_idx,
    std::optional<String> & res_database,
    std::optional<QualifiedTableName> & res_qualified_table_name) const
{
    res_database.reset();
    res_qualified_table_name.reset();

    String str;
    if (!tryGetStringFromArgument(arg_idx, &str, /* allow_identifier= */ true))
        return false;

    if (str.empty())
    {
        res_database = "";
        return true;
    }

    auto qualified_table_name = QualifiedTableName::tryParseFromString(str);
    if (!qualified_table_name)
        return false;

    if (qualified_table_name->database.empty())
        res_database = std::move(qualified_table_name->table);
    else
        res_qualified_table_name = std::move(qualified_table_name);
    return true;
}

void FunctionSecretArgumentsFinder::findEncryptionFunctionSecretArguments()
{
    if (function->arguments->size() == 0)
        return;

    /// We replace all arguments after 'mode' with '[HIDDEN]':
    /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')
    result.start = 1;
    result.count = function->arguments->size() - 1;
}

void FunctionSecretArgumentsFinder::findHMACSecretArguments()
{
    if (function->arguments->size() < 3)
        return;

    /// We hide the key argument and any following for the case of mistyping or using extra arguments by mistake:
    /// HMAC('mode', 'message', 'key') -> HMAC('mode', 'message', '[HIDDEN]')
    /// HMAC('sha256', toString(toFixedString('b', 3), 3), '(', 'this_should_be_secret') -> HMAC('sha256', toString(toFixedString('b', 3), 3), '[HIDDEN]', '[HIDDEN]')
    result.start = 2;
    result.count = function->arguments->size() - 2;
}

void FunctionSecretArgumentsFinder::findTableEngineSecretArguments()
{
    const String & engine_name = function->name();
    if (engine_name == "ExternalDistributed")
    {
        /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
        findExternalDistributedTableEngineSecretArguments();
    }
    else if ((engine_name == "MySQL") || (engine_name == "PostgreSQL") || (engine_name == "MaterializedPostgreSQL"))
    {
        /// MySQL('host:port', 'database', 'table', 'user', 'password', ...)
        /// PostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
        /// MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
        /// MongoDB('host:port', 'database', 'collection', 'user', 'password', ...)
        findMySQLFunctionSecretArguments();
    }
    else if (engine_name == "MongoDB")
    {
        findMongoDBSecretArguments();
    }
    else if ((engine_name == "S3") || (engine_name == "COSN") || (engine_name == "OSS") || (engine_name == "GCS")
             || (engine_name == "DeltaLake") || (engine_name == "DeltaLakeS3") || (engine_name == "Hudi")
             || (engine_name == "Iceberg") || (engine_name == "IcebergS3")
             || (engine_name == "Paimon") || (engine_name == "PaimonS3")
             || (engine_name == "S3Queue"))
    {
        /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
        findS3TableEngineSecretArguments();
    }
    else if (engine_name == "URL")
    {
        findURLSecretArguments();
    }
    else if (engine_name == "AzureBlobStorage" || engine_name == "AzureQueue")
    {
        findAzureBlobStorageTableEngineSecretArguments();
    }
    else if (engine_name == "Redis")
    {
        findRedisTableEngineSecretArguments();
    }
    else if (engine_name == "YTsaurus")
    {
        findYTsaurusStorageTableEngineSecretArguments();
    }
    else if (engine_name == "ArrowFlight")
    {
        findArrowFlightSecretArguments();
    }
    else if ((engine_name == "Remote") || (engine_name == "RemoteSecure"))
    {
        /// Remote('addresses_expr', db, table, 'user', 'password', ...)
        /// RemoteSecure(...) - same as Remote(...)
        /// The arguments are identical to the `remote`/`remoteSecure` table functions, so reuse
        /// the same finder (it also handles the named-collection form `Remote(named_collection, ...)`).
        findRemoteFunctionSecretArguments();
    }
    else if (engine_name == "NATS")
    {
        /// NATS(named_collection, nats_password = 'password', nats_credentials = '...', ...)
        findNATSTableEngineSecretArguments();
    }
    else if ((engine_name == "JDBC") || (engine_name == "ODBC"))
    {
        /// JDBC('DSN', database, table)
        /// ODBC('DSN', database, table)
        /// The DSN (connection string) may contain credentials.
        findXDBCSecretArguments();
    }
}

void FunctionSecretArgumentsFinder::findNATSTableEngineSecretArguments()
{
    /// NATS(named_collection [, nats_password = 'password'] [, nats_token = 'token']
    ///      [, nats_credential_file = '/path'] [, nats_credentials = 'user JWT and seed']
    ///      [, nats_url = 'nats://user:password@host:4222'], ...)
    /// The only positional argument the engine accepts is the name of a named collection, so the
    /// credentials can only appear as named overrides. The `SETTINGS` clause form is masked
    /// separately by `NATS::SETTINGS_TO_HIDE`, and this function masks the same keys the same way:
    /// the secrets are hidden whole, while `nats_url` keeps everything but its userinfo password.
    /// Fail closed on a key we cannot read as a plain literal: it can name a secret setting.
    for (size_t i = 0; i < function->arguments->size(); ++i)
    {
        const auto equals_func = function->arguments->at(i)->getFunction();
        if (!equals_func || equals_func->name() != "equals" || !equals_func->hasArguments()
            || equals_func->arguments->size() != 2)
        {
            /// The engine accepts no positional arguments except the collection name in the first
            /// position, but it rejects them only after the query has been formatted for logging.
            /// A malformed positional argument can carry a secret (a credential file path, a url
            /// with a password), so hide it whole rather than leak it (fail closed).
            if (i > 0 || !function->arguments->at(i)->isIdentifier())
                markSecretArgument(i, /* argument_is_named= */ false);
            continue;
        }

        String key;
        if (!equals_func->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
        {
            markSecretArgument(i, /* argument_is_named= */ true);
        }
        else if (key == "nats_url")
        {
            String url;
            if (equals_func->arguments->at(1)->tryGetString(&url, /* allow_identifier= */ false))
            {
                if (maskURIPassword(&url))
                    result.replaced_arguments[i] = "nats_url = " + quoteString(url);
            }
            else
            {
                /// A url built from a constant expression can embed credentials in its pieces, which
                /// we cannot evaluate here; hide it whole rather than leak.
                markSecretArgument(i, /* argument_is_named= */ true);
            }
        }
        else if (std::find(std::begin(nats_secret_keys), std::end(nats_secret_keys), key) != std::end(nats_secret_keys))
        {
            markSecretArgument(i, /* argument_is_named= */ true);
        }
    }
}

void FunctionSecretArgumentsFinder::findExternalDistributedTableEngineSecretArguments()
{
    if (isNamedCollectionName(1))
    {
        /// ExternalDistributed('engine', named_collection, ..., password = 'password', ...)
        findSecretNamedArgument("password", 2);
    }
    else
    {
        /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
        markSecretArgument(5);
    }
}

void FunctionSecretArgumentsFinder::findS3TableEngineSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// S3(named_collection, ..., secret_access_key = 'secret_access_key')
        findS3NamedCollectionSecretArguments(1);
        return;
    }

    const auto positional = classifyS3Arguments();
    maskS3UrlArgument(positional, 0);

    /// The table engine takes its structure from the column list, never as an argument.
    maskS3PositionalSecrets(positional, 0, /* with_structure= */ false);
}

void FunctionSecretArgumentsFinder::findAzureBlobStorageTableEngineSecretArguments()
{
   /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format, [account_name, account_key, ...])
    size_t url_arg_idx = 0;

    if (isNamedCollectionName(url_arg_idx))
    {
        /// AzureBlobStorage(named_collection, ..., account_key = 'account_key', ...)
        if (maskAzureConnectionString(-1, true, 1))
            return;
        findSecretNamedArgument("account_key", 1);
        return;
    }

    if (maskAzureConnectionString(url_arg_idx))
        return;

    /// We should check other arguments first because we don't need to do any replacement in case of
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    size_t count = function->arguments->size();
    if ((url_arg_idx + 4 <= count) && (count <= url_arg_idx + 7))
    {
        String fourth_arg;
        if (tryGetStringFromArgument(url_arg_idx + 3, &fourth_arg))
        {
            if (fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg))
                return;
        }
    }

    /// We're going to replace 'account_key' with '[HIDDEN]' if account_key is used in the signature
    if (url_arg_idx + 4 < count)
        markSecretArgument(url_arg_idx + 4);
}

void FunctionSecretArgumentsFinder::findRedisFunctionSecretArguments()
{
    // redis(host:port, key, structure, db_index, password, pool_size)
    markSecretArgument(4);
}

void FunctionSecretArgumentsFinder::findYTsaurusStorageTableEngineSecretArguments()
{
    // YTsaurus('base_uri', 'yt_path', 'auth_token')
    markSecretArgument(2);
}

void FunctionSecretArgumentsFinder::findDatabaseEngineSecretArguments()
{
    const String & engine_name = function->name();
    if (engine_name == "MySQL" ||
        engine_name == "PostgreSQL" ||
        engine_name == "MaterializedPostgreSQL")
    {
        /// MySQL('host:port', 'database', 'user', 'password')
        /// PostgreSQL('host:port', 'database', 'user', 'password')
        findMySQLDatabaseSecretArguments();
    }
    else if (engine_name == "S3")
    {
        /// S3('url', 'access_key_id', 'secret_access_key')
        findS3DatabaseSecretArguments();
    }
    else if (engine_name == "DataLakeCatalog")
    {
        findDataLakeCatalogSecretArguments();
    }
    else if (engine_name == "Backup")
    {
        findBackupDatabaseSecretArguments();
    }
}

void FunctionSecretArgumentsFinder::findMySQLDatabaseSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// MySQL(named_collection, ..., password = 'password', ...)
        findSecretNamedArgument("password", 1);
    }
    else
    {
        /// MySQL('host:port', 'database', 'user', 'password')
        markSecretArgument(3);
    }
}

void FunctionSecretArgumentsFinder::findS3DatabaseSecretArguments()
{
    if (isNamedCollectionName(0))
    {
        /// S3(named_collection, ..., secret_access_key = 'password', ...)
        findS3NamedCollectionSecretArguments(1);
    }
    else
    {
        /// S3('url', 'access_key_id', 'secret_access_key' [, session_token = ..., google_adc_* = ...]):
        /// the engine accepts no positional argument beyond secret_access_key, so fail closed from
        /// slot 2 on. Non-secret named overrides (e.g. `use_environment_credentials = 1`) stay visible.
        const auto positional = classifyS3Arguments();
        maskS3UrlArgument(positional, 0);
        maskS3PositionalsFrom(positional, 2);
    }
}

void FunctionSecretArgumentsFinder::findDataLakeCatalogSecretArguments()
{
    /// datalake catalog should support different storage types,
    /// we need a function to check if the url is S3 or Azure.
    /// right now we assume it's a S3 url
    findS3DatabaseSecretArguments();
}

void FunctionSecretArgumentsFinder::findBackupDatabaseSecretArguments()
{
    if (function->arguments->size() < 2)
        return;

    auto storage_arg = function->arguments->at(1);
    auto storage_function = storage_arg->getFunction();

    /// The nested S3 destination is not recognized as an S3 engine when the formatter recurses into it,
    /// so its secrets must be masked here. Handle both forms:
    ///   Backup('', S3('url', 'access_key_id', 'secret_access_key' [, ...]))
    ///   Backup('', S3(named_collection, ..., secret_access_key = '...', session_token = '...', ...))
    /// by reconstructing the nested `S3(...)` with the secret arguments replaced by `[HIDDEN]`.
    if (!storage_function || storage_function->name() != "S3" || !storage_function->hasArguments())
        return;

    const auto & nested_args = *storage_function->arguments;
    const bool is_named_collection = nested_args.size() >= 1 && nested_args.at(0)->isIdentifier();

    /// Count the positional arguments first (everything that is not `key = value` or a nested map):
    /// the visibility rule below depends on the total, mirroring `BackupInfo::fromAST`, which collects
    /// positionals independently of named overrides.
    size_t total_positionals = 0;
    for (size_t i = 0; i < nested_args.size(); ++i)
    {
        const auto f = nested_args.at(i)->getFunction();
        if (f && (f->name() == "extra_credentials"
                  || (f->name() == "equals" && f->hasArguments() && f->arguments->size() == 2)))
            continue;
        ++total_positionals;
    }

    /// Named-collection locator: slot 0 is the collection and slot 1 the non-secret filename.
    /// Explicit-url locator: valid signatures have one positional (the url) or three (url,
    /// access_key_id, secret_access_key) with the secret at slot 2; any other count is invalid and
    /// the intended slots are unknowable, so everything after the url is hidden (fail closed).
    const size_t first_hidden_slot = (is_named_collection || total_positionals == 3) ? 2 : 1;

    std::string replacement = "S3(";
    bool has_secret = false;
    size_t positional_slot = 0;
    for (size_t i = 0; i < nested_args.size(); ++i)
    {
        if (i > 0)
            replacement += ", ";

        auto arg = nested_args.at(i);

        /// Named argument `key = value`.
        if (auto key_value = arg->getFunction();
            key_value && key_value->name() == "equals" && key_value->hasArguments() && key_value->arguments->size() == 2)
        {
            String key;
            if (key_value->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
            {
                const bool is_secret = std::find(std::begin(s3_secret_keys), std::end(s3_secret_keys), key) != std::end(s3_secret_keys);
                replacement += key;
                replacement += " = ";
                String value;
                if (is_secret)
                {
                    replacement += "'[HIDDEN]'";
                    has_secret = true;
                }
                else if (key_value->arguments->at(1)->tryGetString(&value, /* allow_identifier= */ true))
                {
                    /// A `url` override can itself carry credentials (userinfo, presign parameters).
                    has_secret |= maskS3URICredentials(value);
                    replacement += quoteString(value);
                }
                else if (String literal_text; key_value->arguments->at(1)->tryGetLiteralText(&literal_text))
                {
                    /// A non-string scalar override, e.g. `use_environment_credentials = 1`.
                    replacement += literal_text;
                }
                else
                {
                    /// Any remaining value is an expression, not a plain literal or identifier: a `url`
                    /// built from pieces, or a nested `headers(...)` / `extra_credentials(...)` map or
                    /// other function whose formatted text would carry its secrets verbatim (the parser
                    /// evaluates it as a constant, so it is not masked as a nested map here). We cannot
                    /// evaluate it, so hide it rather than leak. This counts as a secret: otherwise a
                    /// replacement whose only hidden part is this value would be discarded below and the
                    /// original expression formatted verbatim.
                    replacement += "'[HIDDEN]'";
                    has_secret = true;
                }
            }
            else
            {
                /// The key is a constant expression the parser would evaluate, so it can name any
                /// secret key; fail closed and hide the whole argument.
                replacement += "'[HIDDEN]'";
                has_secret = true;
            }
            continue;
        }

        /// Nested `extra_credentials(k = v, ...)` map: reconstruct with every value hidden. Build into
        /// a temporary; if any inner key is not a plain literal (e.g. a constant expression the parser
        /// still accepts), fail closed by hiding the whole map rather than emitting it verbatim.
        if (auto extra_credentials_func = arg->getFunction();
            extra_credentials_func && extra_credentials_func->name() == "extra_credentials" && extra_credentials_func->hasArguments())
        {
            std::string masked_map = "extra_credentials(";
            bool reconstructed = true;
            const auto & cred_args = *extra_credentials_func->arguments;
            for (size_t j = 0; j < cred_args.size(); ++j)
            {
                String cred_key;
                auto cred_kv = cred_args.at(j)->getFunction();
                if (cred_kv && cred_kv->name() == "equals" && cred_kv->hasArguments() && cred_kv->arguments->size() == 2
                    && cred_kv->arguments->at(0)->tryGetString(&cred_key, /* allow_identifier= */ true))
                {
                    if (j > 0)
                        masked_map += ", ";
                    String cred_value;
                    if (isNonSecretExtraCredentialsKey(cred_key)
                        && cred_kv->arguments->at(1)->tryGetString(&cred_value, /* allow_identifier= */ true))
                        masked_map += cred_key + " = " + quoteString(cred_value);
                    else
                        masked_map += cred_key + " = '[HIDDEN]'";
                }
                else
                {
                    reconstructed = false;
                    break;
                }
            }
            masked_map += ")";
            replacement += reconstructed ? masked_map : "'[HIDDEN]'";
            has_secret = true;
            continue;
        }

        /// Positional argument: the slot is counted over positionals only, and its visibility follows
        /// the signature rule computed above.
        const size_t slot = positional_slot++;
        if (slot >= first_hidden_slot)
        {
            replacement += "'[HIDDEN]'";
            has_secret = true;
            continue;
        }

        String arg_value;
        if (arg->isIdentifier() && arg->tryGetString(&arg_value, /* allow_identifier= */ true))
            replacement += arg_value; /// e.g. the named collection name, kept unquoted.
        else if (arg->tryGetString(&arg_value, /* allow_identifier= */ true))
        {
            /// The url positional can itself carry credentials (userinfo, presign parameters).
            has_secret |= maskS3URICredentials(arg_value);
            replacement += quoteString(arg_value);
        }
        else
        {
            /// Fail closed: an argument we cannot reconstruct safely (e.g. an unsupported tail like
            /// `headers(..)`, or a non-literal expression) must not be emitted verbatim. Hide it.
            replacement += "'[HIDDEN]'";
            has_secret = true;
        }
    }
    replacement += ")";

    if (!has_secret)
        return;

    result.start = 1;
    result.count = 1;
    result.replacement = std::move(replacement);
    result.quote_replacement = false;
}

void FunctionSecretArgumentsFinder::findBackupNameSecretArguments()
{
    const String & engine_name = function->name();
    if (engine_name == "S3")
    {
        if (isNamedCollectionName(0))
        {
            /// BACKUP ... TO S3(named_collection[, 'filename'], ..., secret_access_key = '...', ...):
            /// unlike the other named-collection S3 forms, the backup locator accepts one positional
            /// (the non-secret filename), in any position relative to the named overrides; anything
            /// positional beyond it is invalid, so fail closed there.
            maskS3PositionalsFrom(classifyS3Arguments(1, /* positionals_allowed_after_named= */ true), 1);
            return;
        }
        /// BACKUP ... TO S3(url [, aws_access_key_id, aws_secret_access_key] [, session_token = ..., ...]):
        /// the locator accepts exactly one or three positionals; the valid triple keeps the url and
        /// access_key_id visible and hides the secret at slot 2. Any other positional count is invalid
        /// but logged before validation, and the intended slots are unknowable, so fail closed on
        /// everything after the url.
        const auto positional = classifyS3Arguments(0, /* positionals_allowed_after_named= */ true);
        maskS3UrlArgument(positional, 0);
        maskS3PositionalsFrom(positional, positional.size() == 3 ? 2 : 1);
    }
    else if (engine_name == "AzureBlobStorage" || engine_name == "AzureQueue")
    {
        findAzureBlobStorageTableEngineSecretArguments();
    }
}

bool FunctionSecretArgumentsFinder::isNamedCollectionName(size_t arg_idx) const
{
    if (function->arguments->size() <= arg_idx)
        return false;

    return function->arguments->at(arg_idx)->isIdentifier();
}

ssize_t FunctionSecretArgumentsFinder::findNamedArgument(String * res, std::string_view key, size_t start)
{
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        const auto & argument = function->arguments->at(i);
        const auto equals_func = argument->getFunction();
        if (!equals_func || (equals_func->name() != "equals"))
            continue;

        if (!equals_func->arguments || equals_func->arguments->size() != 2)
            continue;

        String found_key;
        if (!tryGetStringFromArgument(*equals_func->arguments->at(0), &found_key))
            continue;

        if (found_key == key)
        {
            tryGetStringFromArgument(*equals_func->arguments->at(1), res);
            return i;
        }
    }

    return -1;
}

bool FunctionSecretArgumentsFinder::findSecretNamedArgument(std::string_view key, size_t start)
{
    bool found = false;
    for (ssize_t arg_idx = findNamedArgument(nullptr, key, start); arg_idx >= 0;
         arg_idx = findNamedArgument(nullptr, key, static_cast<size_t>(arg_idx) + 1))
    {
        markSecretArgument(arg_idx, /* argument_is_named= */ true);
        found = true;
    }
    return found;
}

void FunctionSecretArgumentsFinder::findS3NamedCollectionSecretArguments(size_t start)
{
    /// After the collection name every argument must be a named `option = value` override or a nested
    /// map; a positional argument is invalid but logged before validation rejects it, so fail closed
    /// and hide every positional the classification returns.
    maskS3PositionalsFrom(classifyS3Arguments(start), 0);
}

}
