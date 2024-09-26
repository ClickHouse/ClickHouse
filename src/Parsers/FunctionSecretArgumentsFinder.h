#pragma once

#include <Common/KnownObjectNames.h>
#include <Common/re2.h>
#include <Common/maskURIPassword.h>
#include <Core/QualifiedTableName.h>
#include <base/defines.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{

class AbstractFunction
{
    friend class FunctionSecretArgumentsFinder;
public:
    class Argument
    {
    public:
        virtual ~Argument() = default;
        virtual std::unique_ptr<AbstractFunction> getFunction() const = 0;
        virtual bool isIdentifier() const = 0;
        virtual bool tryGetString(String * res, bool allow_identifier) const = 0;
    };
    class Arguments
    {
    public:
        virtual ~Arguments() = default;
        virtual size_t size() const = 0;
        virtual std::unique_ptr<Argument> at(size_t n) const = 0;
    };

    virtual ~AbstractFunction() = default;
    virtual String name() const = 0;
    bool hasArguments() const { return !!arguments; }

protected:
    std::unique_ptr<Arguments> arguments;
};

class FunctionSecretArgumentsFinder
{
public:
    struct Result
    {
        /// Result constructed by default means no arguments will be hidden.
        size_t start = static_cast<size_t>(-1);
        size_t count = 0; /// Mostly it's either 0 or 1. There are only a few cases where `count` can be greater than 1 (e.g. see `encrypt`).
                            /// In all known cases secret arguments are consecutive
        bool are_named = false; /// Arguments like `password = 'password'` are considered as named arguments.
        /// E.g. "headers" in `url('..', headers('foo' = '[HIDDEN]'))`
        std::vector<std::string> nested_maps;
        /// Full replacement of an argument. Only supported when count is 1, otherwise all arguments will be replaced with this string.
        /// It's needed in cases when we don't want to hide the entire parameter, but some part of it, e.g. "connection_string" in
        /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=secretkey;...', ...)` should be replaced with
        /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=[HIDDEN];...', ...)`.
        std::string replacement;

        bool hasSecrets() const
        {
            return count != 0 || !nested_maps.empty();
        }
    };

    explicit FunctionSecretArgumentsFinder(std::unique_ptr<AbstractFunction> && function_) : function(std::move(function_)) {}

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }

protected:
    const std::unique_ptr<AbstractFunction> function;
    Result result;

    void markSecretArgument(size_t index, bool argument_is_named = false)
    {
        if (index >= function->arguments->size())
            return;
        if (!result.count)
        {
            result.start = index;
            result.are_named = argument_is_named;
        }
        chassert(index >= result.start); /// We always check arguments consecutively
        chassert(result.replacement.empty()); /// We shouldn't use replacement with masking other arguments
        result.count = index + 1 - result.start;
        if (!argument_is_named)
            result.are_named = false;
    }

    void findOrdinaryFunctionSecretArguments()
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
                 (function->name() == "deltaLake") || (function->name() == "hudi") || (function->name() == "iceberg") ||
                 (function->name() == "gcs"))
        {
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            findS3FunctionSecretArguments(/* is_cluster_function= */ false);
        }
        else if (function->name() == "s3Cluster")
        {
            /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            findS3FunctionSecretArguments(/* is_cluster_function= */ true);
        }
        else if (function->name() == "azureBlobStorage")
        {
            /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure)
            findAzureBlobStorageFunctionSecretArguments(/* is_cluster_function= */ false);
        }
        else if (function->name() == "azureBlobStorageCluster")
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
        else if (function->name() == "url")
        {
            findURLSecretArguments();
        }
    }

    void findMySQLFunctionSecretArguments()
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

    void findMongoDBSecretArguments()
    {
        String uri;

        if (isNamedCollectionName(0))
        {
            /// MongoDB(named_collection, ..., password = 'password', ...)
            if (findSecretNamedArgument("password", 1))
                return;

            /// MongoDB(named_collection, ..., uri = 'mongodb://username:password@127.0.0.1:27017', ...)
            findNamedArgument(&uri, "uri", 1);
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

    /// Returns the number of arguments excluding "headers" and "extra_credentials" (which should
    /// always be at the end). Marks "headers" as secret, if found.
    size_t excludeS3OrURLNestedMaps()
    {
        size_t count = function->arguments->size();
        while (count > 0)
        {
            const auto f = function->arguments->at(count - 1)->getFunction();
            if (!f)
                break;
            if (f->name() == "headers")
                result.nested_maps.push_back(f->name());
            else if (f->name() != "extra_credentials")
                break;
            count -= 1;
        }
        return count;
    }

    void findS3FunctionSecretArguments(bool is_cluster_function)
    {
        /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
        size_t url_arg_idx = is_cluster_function ? 1 : 0;

        if (!is_cluster_function && isNamedCollectionName(0))
        {
            /// s3(named_collection, ..., secret_access_key = 'secret_access_key', ...)
            findSecretNamedArgument("secret_access_key", 1);
            return;
        }

        /// We should check other arguments first because we don't need to do any replacement in case of
        /// s3('url', NOSIGN, 'format' [, 'compression'] [, extra_credentials(..)] [, headers(..)])
        /// s3('url', 'format', 'structure' [, 'compression'] [, extra_credentials(..)] [, headers(..)])
        size_t count = excludeS3OrURLNestedMaps();
        if ((url_arg_idx + 3 <= count) && (count <= url_arg_idx + 4))
        {
            String second_arg;
            if (tryGetStringFromArgument(url_arg_idx + 1, &second_arg))
            {
                if (boost::iequals(second_arg, "NOSIGN"))
                    return; /// The argument after 'url' is "NOSIGN".

                if (second_arg == "auto" || KnownFormatNames::instance().exists(second_arg))
                    return; /// The argument after 'url' is a format: s3('url', 'format', ...)
            }
        }

        /// We're going to replace 'aws_secret_access_key' with '[HIDDEN]' for the following signatures:
        /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
        /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
        if (url_arg_idx + 2 < count)
            markSecretArgument(url_arg_idx + 2);
    }

    void findAzureBlobStorageFunctionSecretArguments(bool is_cluster_function)
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
        else if (is_cluster_function && isNamedCollectionName(1))
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
        /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format, [account_name, account_key, ...])
        /// azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, format, [account_name, account_key, ...])
        size_t count = function->arguments->size();
        if ((url_arg_idx + 4 <= count) && (count <= url_arg_idx + 7))
        {
            String fourth_arg;
            if (tryGetStringFromArgument(url_arg_idx + 3, &fourth_arg))
            {
                if (fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg))
                    return; /// The argument after 'url' is a format: s3('url', 'format', ...)
            }
        }

        /// We're going to replace 'account_key' with '[HIDDEN]' if account_key is used in the signature
        if (url_arg_idx + 4 < count)
            markSecretArgument(url_arg_idx + 4);
    }

    bool maskAzureConnectionString(ssize_t url_arg_idx, bool argument_is_named = false, size_t start = 0)
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
            static re2::RE2 account_key_pattern = "AccountKey=.*?(;|$)";
            if (RE2::Replace(&url_arg, account_key_pattern, "AccountKey=[HIDDEN]\\1"))
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

    void findURLSecretArguments()
    {
        if (!isNamedCollectionName(0))
            excludeS3OrURLNestedMaps();
    }

    bool tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier = true) const
    {
        if (arg_idx >= function->arguments->size())
            return false;

        return tryGetStringFromArgument(*function->arguments->at(arg_idx), res, allow_identifier);
    }

    static bool tryGetStringFromArgument(const AbstractFunction::Argument & argument, String * res, bool allow_identifier = true)
    {
        return argument.tryGetString(res, allow_identifier);
    }

    void findRemoteFunctionSecretArguments()
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

    /// Tries to get either a database name or a qualified table name from an argument.
    /// Empty string is also allowed (it means the default database).
    /// The function is used by findRemoteFunctionSecretArguments() to determine how many arguments to skip before a password.
    bool tryGetDatabaseNameOrQualifiedTableName(
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

    void findEncryptionFunctionSecretArguments()
    {
        if (function->arguments->size() == 0)
            return;

        /// We replace all arguments after 'mode' with '[HIDDEN]':
        /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')
        result.start = 1;
        result.count = function->arguments->size() - 1;
    }

    void findTableEngineSecretArguments()
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
        else if ((engine_name == "S3") || (engine_name == "COSN") || (engine_name == "OSS") ||
                    (engine_name == "DeltaLake") || (engine_name == "Hudi") || (engine_name == "Iceberg") || (engine_name == "S3Queue"))
        {
            /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
            findS3TableEngineSecretArguments();
        }
        else if (engine_name == "URL")
        {
            findURLSecretArguments();
        }
    }

    void findExternalDistributedTableEngineSecretArguments()
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

    void findS3TableEngineSecretArguments()
    {
        if (isNamedCollectionName(0))
        {
            /// S3(named_collection, ..., secret_access_key = 'secret_access_key')
            findSecretNamedArgument("secret_access_key", 1);
            return;
        }

        /// We should check other arguments first because we don't need to do any replacement in case of
        /// S3('url', NOSIGN, 'format' [, 'compression'] [, extra_credentials(..)] [, headers(..)])
        /// S3('url', 'format', 'compression' [, extra_credentials(..)] [, headers(..)])
        size_t count = excludeS3OrURLNestedMaps();
        if ((3 <= count) && (count <= 4))
        {
            String second_arg;
            if (tryGetStringFromArgument(1, &second_arg))
            {
                if (boost::iequals(second_arg, "NOSIGN"))
                    return; /// The argument after 'url' is "NOSIGN".

                if (count == 3)
                {
                    if (second_arg == "auto" || KnownFormatNames::instance().exists(second_arg))
                        return; /// The argument after 'url' is a format: S3('url', 'format', ...)
                }
            }
        }

        /// We replace 'aws_secret_access_key' with '[HIDDEN]' for the following signatures:
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
        if (2 < count)
            markSecretArgument(2);
    }

    void findDatabaseEngineSecretArguments()
    {
        const String & engine_name = function->name();
        if ((engine_name == "MySQL") || (engine_name == "MaterializeMySQL") ||
            (engine_name == "MaterializedMySQL") || (engine_name == "PostgreSQL") ||
            (engine_name == "MaterializedPostgreSQL"))
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
    }

    void findMySQLDatabaseSecretArguments()
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

    void findS3DatabaseSecretArguments()
    {
        if (isNamedCollectionName(0))
        {
            /// S3(named_collection, ..., secret_access_key = 'password', ...)
            findSecretNamedArgument("secret_access_key", 1);
        }
        else
        {
            /// S3('url', 'access_key_id', 'secret_access_key')
            markSecretArgument(2);
        }
    }

    void findBackupNameSecretArguments()
    {
        const String & engine_name = function->name();
        if (engine_name == "S3")
        {
            /// BACKUP ... TO S3(url, [aws_access_key_id, aws_secret_access_key])
            markSecretArgument(2);
        }
    }

    /// Whether a specified argument can be the name of a named collection?
    bool isNamedCollectionName(size_t arg_idx) const
    {
        if (function->arguments->size() <= arg_idx)
            return false;

        return function->arguments->at(arg_idx)->isIdentifier();
    }

    /// Looks for an argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
    /// Returns -1 if no argument was found.
    ssize_t findNamedArgument(String * res, const std::string_view & key, size_t start = 0)
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

    /// Looks for a secret argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
    /// If the argument is found, it is marked as a secret.
    bool findSecretNamedArgument(const std::string_view & key, size_t start = 0)
    {
        ssize_t arg_idx = findNamedArgument(nullptr, key, start);
        if (arg_idx >= 0)
        {
            markSecretArgument(arg_idx, /* argument_is_named= */ true);
            return true;
        }
        return false;
    }
};

}
