#include <Parsers/findFunctionSecretArguments.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/KnownObjectNames.h>
#include <Core/QualifiedTableName.h>


namespace DB
{

namespace
{
    constexpr const std::pair<size_t, size_t> npos{static_cast<size_t>(-1), static_cast<size_t>(-1)};

    bool tryGetStringFromArgument(const ASTFunction & function, size_t arg_idx, String * res, bool allow_literal, bool allow_identifier)
    {
        if (!function.arguments)
            return false;

        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return false; /// return false because we don't want to validate query here

        const auto & arguments = expr_list->children;
        if (arg_idx >= arguments.size())
            return false;

        ASTPtr argument = arguments[arg_idx];
        if (allow_literal)
        {
            if (const auto * literal = argument->as<ASTLiteral>())
            {
                if (literal->value.getType() != Field::Types::String)
                    return false;
                if (res)
                    *res = literal->value.safeGet<String>();
                return true;
            }
        }

        if (allow_identifier)
        {
            if (const auto * id = argument->as<ASTIdentifier>())
            {
                if (res)
                    *res = id->name();
                return true;
            }
        }

        return false;
    }


    std::pair<size_t, size_t> findS3FunctionSecretArguments(const ASTFunction & function, bool is_cluster_function)
    {
        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return npos; /// return because we don't want to validate query here

        const auto & arguments = expr_list->children;

        /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
        size_t url_arg_idx = is_cluster_function ? 1 : 0;

        /// We're going to replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
        /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
        /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

        /// But we should check the number of arguments first because we don't need to do any replacements in case of
        /// s3('url' [, 'format']) or s3Cluster('cluster_name', 'url' [, 'format'])
        if (arguments.size() < url_arg_idx + 3)
            return npos;

        if (arguments.size() >= url_arg_idx + 5)
        {
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'structure', ...)
            return {url_arg_idx + 2, url_arg_idx + 3};
        }
        else
        {
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            /// We need to distinguish that from s3('url', 'format', 'structure' [, 'compression_method']).
            /// So we will check whether the argument after 'url' is a format.
            String format;
            if (!tryGetStringFromArgument(function, url_arg_idx + 1, &format, /* allow_literal= */ true, /* allow_identifier= */ false))
            {
                /// We couldn't evaluate the argument after 'url' so we don't know whether it is a format or `aws_access_key_id`.
                /// So it's safer to wipe the next argument just in case.
                return {url_arg_idx + 2, url_arg_idx + 3}; /// Wipe either `aws_secret_access_key` or `structure`.
            }

            if (KnownFormatNames::instance().exists(format))
                return npos; /// The argument after 'url' is a format: s3('url', 'format', ...)

            /// The argument after 'url' is not a format so we do our replacement:
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...) -> s3('url', 'aws_access_key_id', '[HIDDEN]', ...)
            return {url_arg_idx + 2, url_arg_idx + 3};
        }
    }


    std::pair<size_t, size_t> findRemoteFunctionSecretArguments(const ASTFunction & function)
    {
        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return npos; /// return because we don't want to validate query here

        const auto & arguments = expr_list->children;

        /// We're going to replace 'password' with '[HIDDEN'] for the following signatures:
        /// remote('addresses_expr', db.table, 'user' [, 'password'] [, sharding_key])
        /// remote('addresses_expr', 'db', 'table', 'user' [, 'password'] [, sharding_key])
        /// remote('addresses_expr', table_function(), 'user' [, 'password'] [, sharding_key])

        /// But we should check the number of arguments first because we don't need to do any replacements in case of
        /// remote('addresses_expr', db.table)
        if (arguments.size() < 3)
            return npos;

        size_t arg_num = 1;

        /// Skip 1 or 2 arguments with table_function() or db.table or 'db', 'table'.
        const auto * table_function = arguments[arg_num]->as<ASTFunction>();
        if (table_function && KnownTableFunctionNames::instance().exists(table_function->name))
        {
            ++arg_num;
        }
        else
        {
            String database;
            if (!tryGetStringFromArgument(function, arg_num, &database, /* allow_literal= */ true, /* allow_identifier= */ true))
            {
                /// We couldn't evaluate the argument so we don't know whether it is 'db.table' or just 'db'.
                /// Hence we can't figure out whether we should skip one argument 'user' or two arguments 'table', 'user'
                /// before the argument 'password'. So it's safer to wipe two arguments just in case.
                /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
                /// before wiping it (because the `password` argument is always a literal string).
                auto res = npos;
                if (tryGetStringFromArgument(function, arg_num + 2, nullptr, /* allow_literal= */ true, /* allow_identifier= */ false))
                {
                    /// Wipe either `password` or `user`.
                    res = {arg_num + 2, arg_num + 3};
                }
                if (tryGetStringFromArgument(function, arg_num + 3, nullptr, /* allow_literal= */ true, /* allow_identifier= */ false))
                {
                    /// Wipe either `password` or `sharding_key`.
                    if (res == npos)
                        res.first = arg_num + 3;
                    res.second = arg_num + 4;
                }
                return res;
            }

            ++arg_num;
            auto qualified_name = QualifiedTableName::parseFromString(database);
            if (qualified_name.database.empty())
                ++arg_num; /// skip 'table' argument
        }

        /// Skip username.
        ++arg_num;

        /// Do our replacement:
        /// remote('addresses_expr', db.table, 'user', 'password', ...) -> remote('addresses_expr', db.table, 'user', '[HIDDEN]', ...)
        /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
        /// before wiping it (because the `password` argument is always a literal string).
        bool can_be_password = tryGetStringFromArgument(function, arg_num, nullptr, /* allow_literal= */ true, /* allow_identifier= */ false);
        if (can_be_password)
            return {arg_num, arg_num + 1};

        return npos;
    }


    std::pair<size_t, size_t> findEncryptionFunctionSecretArguments(const ASTFunction & function)
    {
        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return npos; /// return because we don't want to validate query here

        const auto & arguments = expr_list->children;

        /// We replace all arguments after 'mode' with '[HIDDEN]':
        /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')
        return {1, arguments.size()};
    }


    std::pair<size_t, size_t> findOrdinaryFunctionSecretArguments(const ASTFunction & function)
    {
        if (function.name == "mysql")
        {
            /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
            return {4, 5};
        }
        else if (function.name == "postgresql")
        {
            /// postgresql('host:port', 'database', 'table', 'user', 'password', ...)
            return {4, 5};
        }
        else if (function.name == "mongodb")
        {
            /// mongodb('host:port', 'database', 'collection', 'user', 'password', ...)
            return {4, 5};
        }
        else if (function.name == "s3" || function.name == "cosn" || function.name == "oss")
        {
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            return findS3FunctionSecretArguments(function, /* is_cluster_function= */ false);
        }
        else if (function.name == "s3Cluster")
        {
            /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            return findS3FunctionSecretArguments(function, /* is_cluster_function= */ true);
        }
        else if (function.name == "remote" || function.name == "remoteSecure")
        {
            /// remote('addresses_expr', 'db', 'table', 'user', 'password', ...)
            return findRemoteFunctionSecretArguments(function);
        }
        else if (
            function.name == "encrypt" || function.name == "decrypt" || function.name == "aes_encrypt_mysql"
            || function.name == "aes_decrypt_mysql" || function.name == "tryDecrypt")
        {
            /// encrypt('mode', 'plaintext', 'key' [, iv, aad])
            return findEncryptionFunctionSecretArguments(function);
        }
        else
        {
            return npos;
        }
    }


    std::pair<size_t, size_t> findS3TableEngineSecretArguments(const ASTFunction & function)
    {
        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return npos; /// return because we don't want to validate query here

        const auto & arguments = expr_list->children;

        /// We replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

        /// But we should check the number of arguments first because we don't need to do that replacements in case of
        /// S3('url' [, 'format' [, 'compression']])
        if (arguments.size() < 4)
            return npos;

        return {2, 3};
    }


    std::pair<size_t, size_t> findTableEngineSecretArguments(const ASTFunction & function)
    {
        const String & engine_name = function.name;
        if (engine_name == "ExternalDistributed")
        {
            /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
            return {5, 6};
        }
        else if (engine_name == "MySQL")
        {
            /// MySQL('host:port', 'database', 'table', 'user', 'password', ...)
            return {4, 5};
        }
        else if (engine_name == "PostgreSQL")
        {
            /// PostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
            return {4, 5};
        }
        else if (engine_name == "MaterializedPostgreSQL")
        {
            /// MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
            return {4, 5};
        }
        else if (engine_name == "MongoDB")
        {
            /// MongoDB('host:port', 'database', 'collection', 'user', 'password', ...)
            return {4, 5};
        }
        else if (engine_name == "S3" || engine_name == "COSN" || engine_name == "OSS")
        {
            /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
            return findS3TableEngineSecretArguments(function);
        }
        else
        {
            return npos;
        }
    }


    std::pair<size_t, size_t> findDatabaseEngineSecretArguments(const ASTFunction & function)
    {
        const String & engine_name = function.name;
        if (engine_name == "MySQL" || engine_name == "MaterializeMySQL" || engine_name == "MaterializedMySQL")
        {
            /// MySQL('host:port', 'database', 'user', 'password')
            return {3, 4};
        }
        else if (engine_name == "PostgreSQL" || engine_name == "MaterializedPostgreSQL")
        {
            /// PostgreSQL('host:port', 'database', 'user', 'password', ...)
            return {3, 4};
        }
        else
        {
            return npos;
        }
    }


    std::pair<size_t, size_t> findBackupNameSecretArguments(const ASTFunction & function)
    {
        const String & engine_name = function.name;
        if (engine_name == "S3")
        {
            /// BACKUP ... TO S3(url, [aws_access_key_id, aws_secret_access_key])
            return {2, 3};
        }
        else
        {
            return npos;
        }
    }
}

std::pair<size_t, size_t> findFunctionSecretArguments(const ASTFunction & function)
{
    switch (function.kind)
    {
        case ASTFunction::Kind::ORDINARY_FUNCTION: return findOrdinaryFunctionSecretArguments(function);
        case ASTFunction::Kind::WINDOW_FUNCTION: return npos;
        case ASTFunction::Kind::LAMBDA_FUNCTION: return npos;
        case ASTFunction::Kind::TABLE_ENGINE: return findTableEngineSecretArguments(function);
        case ASTFunction::Kind::DATABASE_ENGINE: return findDatabaseEngineSecretArguments(function);
        case ASTFunction::Kind::BACKUP_NAME: return findBackupNameSecretArguments(function);
    }
}

}
