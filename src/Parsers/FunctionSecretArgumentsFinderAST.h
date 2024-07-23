#pragma once

#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/KnownObjectNames.h>

#include <boost/algorithm/string/predicate.hpp>


namespace DB
{


/// Finds arguments of a specified function which should not be displayed for most users for security reasons.
/// That involves passwords and secret keys.
class FunctionSecretArgumentsFinderAST
{
public:
    explicit FunctionSecretArgumentsFinderAST(const ASTFunction & function_) : function(function_)
    {
        if (!function.arguments)
            return;

        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return;

        arguments = &expr_list->children;
        switch (function.kind)
        {
            case ASTFunction::Kind::ORDINARY_FUNCTION: findOrdinaryFunctionSecretArguments(); break;
            case ASTFunction::Kind::WINDOW_FUNCTION: break;
            case ASTFunction::Kind::LAMBDA_FUNCTION:  break;
            case ASTFunction::Kind::TABLE_ENGINE: findTableEngineSecretArguments(); break;
            case ASTFunction::Kind::DATABASE_ENGINE: findDatabaseEngineSecretArguments(); break;
            case ASTFunction::Kind::BACKUP_NAME: findBackupNameSecretArguments(); break;
        }
    }

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }

private:
    const ASTFunction & function;
    const ASTs * arguments = nullptr;
    FunctionSecretArgumentsFinder::Result result;

    void markSecretArgument(size_t index, bool argument_is_named = false)
    {
        if (index >= arguments->size())
            return;
        if (!result.count)
        {
            result.start = index;
            result.are_named = argument_is_named;
        }
        chassert(index >= result.start); /// We always check arguments consecutively
        result.count = index + 1 - result.start;
        if (!argument_is_named)
            result.are_named = false;
    }

    void findOrdinaryFunctionSecretArguments()
    {
        if ((function.name == "mysql") || (function.name == "postgresql") || (function.name == "mongodb"))
        {
            /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
            /// postgresql('host:port', 'database', 'table', 'user', 'password', ...)
            /// mongodb('host:port', 'database', 'collection', 'user', 'password', ...)
            findMySQLFunctionSecretArguments();
        }
        else if ((function.name == "s3") || (function.name == "cosn") || (function.name == "oss") ||
                    (function.name == "deltaLake") || (function.name == "hudi") || (function.name == "iceberg"))
        {
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            findS3FunctionSecretArguments(/* is_cluster_function= */ false);
        }
        else if (function.name == "s3Cluster")
        {
            /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            findS3FunctionSecretArguments(/* is_cluster_function= */ true);
        }
        else if ((function.name == "remote") || (function.name == "remoteSecure"))
        {
            /// remote('addresses_expr', 'db', 'table', 'user', 'password', ...)
            findRemoteFunctionSecretArguments();
        }
        else if ((function.name == "encrypt") || (function.name == "decrypt") ||
                    (function.name == "aes_encrypt_mysql") || (function.name == "aes_decrypt_mysql") ||
                    (function.name == "tryDecrypt"))
        {
            /// encrypt('mode', 'plaintext', 'key' [, iv, aad])
            findEncryptionFunctionSecretArguments();
        }
        else if (function.name == "url")
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

    /// Returns the number of arguments excluding "headers" and "extra_credentials" (which should
    /// always be at the end). Marks "headers" as secret, if found.
    size_t excludeS3OrURLNestedMaps()
    {
        size_t count = arguments->size();
        while (count > 0)
        {
            const ASTFunction * f = arguments->at(count - 1)->as<ASTFunction>();
            if (!f)
                break;
            if (f->name == "headers")
                result.nested_maps.push_back(f->name);
            else if (f->name != "extra_credentials")
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

    void findURLSecretArguments()
    {
        if (!isNamedCollectionName(0))
            excludeS3OrURLNestedMaps();
    }

    bool tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier = true) const
    {
        if (arg_idx >= arguments->size())
            return false;

        return tryGetStringFromArgument(*(*arguments)[arg_idx], res, allow_identifier);
    }

    static bool tryGetStringFromArgument(const IAST & argument, String * res, bool allow_identifier = true)
    {
        if (const auto * literal = argument.as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
                return false;
            if (res)
                *res = literal->value.safeGet<String>();
            return true;
        }

        if (allow_identifier)
        {
            if (const auto * id = argument.as<ASTIdentifier>())
            {
                if (res)
                    *res = id->name();
                return true;
            }
        }

        return false;
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
        if (arguments->size() < 3)
            return;

        size_t arg_num = 1;

        /// Skip 1 or 2 arguments with table_function() or db.table or 'db', 'table'.
        const auto * table_function = (*arguments)[arg_num]->as<ASTFunction>();
        if (table_function && KnownTableFunctionNames::instance().exists(table_function->name))
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
        if (arguments->empty())
            return;

        /// We replace all arguments after 'mode' with '[HIDDEN]':
        /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')
        result.start = 1;
        result.count = arguments->size() - 1;
    }

    void findTableEngineSecretArguments()
    {
        const String & engine_name = function.name;
        if (engine_name == "ExternalDistributed")
        {
            /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
            findExternalDistributedTableEngineSecretArguments();
        }
        else if ((engine_name == "MySQL") || (engine_name == "PostgreSQL") ||
                    (engine_name == "MaterializedPostgreSQL") || (engine_name == "MongoDB"))
        {
            /// MySQL('host:port', 'database', 'table', 'user', 'password', ...)
            /// PostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
            /// MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
            /// MongoDB('host:port', 'database', 'collection', 'user', 'password', ...)
            findMySQLFunctionSecretArguments();
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
        const String & engine_name = function.name;
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
        const String & engine_name = function.name;
        if (engine_name == "S3")
        {
            /// BACKUP ... TO S3(url, [aws_access_key_id, aws_secret_access_key])
            markSecretArgument(2);
        }
    }

    /// Whether a specified argument can be the name of a named collection?
    bool isNamedCollectionName(size_t arg_idx) const
    {
        if (arguments->size() <= arg_idx)
            return false;

        const auto * identifier = (*arguments)[arg_idx]->as<ASTIdentifier>();
        return identifier != nullptr;
    }

    /// Looks for a secret argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
    void findSecretNamedArgument(const std::string_view & key, size_t start = 0)
    {
        for (size_t i = start; i < arguments->size(); ++i)
        {
            const auto & argument = (*arguments)[i];
            const auto * equals_func = argument->as<ASTFunction>();
            if (!equals_func || (equals_func->name != "equals"))
                continue;

            const auto * expr_list = equals_func->arguments->as<ASTExpressionList>();
            if (!expr_list)
                continue;

            const auto & equal_args = expr_list->children;
            if (equal_args.size() != 2)
                continue;

            String found_key;
            if (!tryGetStringFromArgument(*equal_args[0], &found_key))
                continue;

            if (found_key == key)
                markSecretArgument(i, /* argument_is_named= */ true);
        }
    }
};

}
