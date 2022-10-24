#include <Interpreters/maskSensitiveInfoInQueryForLogging.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/formatAST.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/typeid_cast.h>


namespace ProfileEvents
{
    extern const Event QueryMaskingRulesMatch;
}


namespace DB
{

namespace
{
    enum class PasswordWipingMode
    {
        Query,
        BackupName,
    };


    template <bool check_only>
    class PasswordWipingVisitor
    {
    public:
        struct Data
        {
            bool can_contain_password = false;
            bool is_create_table_query = false;
            bool is_create_database_query = false;
            bool is_create_dictionary_query = false;
            ContextPtr context;
            PasswordWipingMode mode = PasswordWipingMode::Query;
        };

        using Visitor = std::conditional_t<
            check_only,
            ConstInDepthNodeVisitor<PasswordWipingVisitor, /* top_to_bottom= */ true, /* need_child_accept_data= */ true>,
            InDepthNodeVisitor<PasswordWipingVisitor, /* top_to_bottom= */ true, /* need_child_accept_data= */ true>>;

        static bool needChildVisit(const ASTPtr & /* ast */, const ASTPtr & /* child */, Data & data)
        {
            if constexpr (check_only)
            {
                return !data.can_contain_password;
            }
            else
            {
                return true;
            }
        }

        static void visit(ASTPtr ast, Data & data)
        {
            if (auto * create_user_query = ast->as<ASTCreateUserQuery>())
            {
                visitCreateUserQuery(*create_user_query, data);
            }
            else if (auto * create_query = ast->as<ASTCreateQuery>())
            {
                visitCreateQuery(*create_query, data);
            }
            else if (auto * backup_query = ast->as<ASTBackupQuery>())
            {
                visitBackupQuery(*backup_query, data);
            }
            else if (auto * storage = ast->as<ASTStorage>())
            {
                if (data.is_create_table_query)
                    visitTableEngine(*storage, data);
                else if (data.is_create_database_query)
                    visitDatabaseEngine(*storage, data);
            }
            else if (auto * dictionary = ast->as<ASTDictionary>())
            {
                if (data.is_create_dictionary_query)
                    visitDictionaryDef(*dictionary, data);
            }
            else if (auto * function = ast->as<ASTFunction>())
            {
                if (data.mode == PasswordWipingMode::BackupName)
                    wipePasswordFromBackupEngineArguments(*function, data);
                else
                    visitFunction(*function, data);
            }
        }

    private:
        static void visitCreateUserQuery(ASTCreateUserQuery & query, Data & data)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }
            query.show_password = false;
        }

        static void visitCreateQuery(ASTCreateQuery & query, Data & data)
        {
            if (query.is_dictionary)
                data.is_create_dictionary_query = true;
            else if (query.table)
                data.is_create_table_query = true;
            else
                data.is_create_database_query = true;
        }

        static void visitTableEngine(ASTStorage & storage, Data & data)
        {
            if (!storage.engine)
                return;

            const String & engine_name = storage.engine->name;

            if (engine_name == "ExternalDistributed")
            {
                /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
                wipePasswordFromArgument(*storage.engine, data, 5);
            }
            else if (engine_name == "MySQL")
            {
                /// MySQL('host:port', 'database', 'table', 'user', 'password', ...)
                wipePasswordFromArgument(*storage.engine, data, 4);
            }
            else if (engine_name == "PostgreSQL")
            {
                /// PostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
                wipePasswordFromArgument(*storage.engine, data, 4);
            }
            else if (engine_name == "MaterializedPostgreSQL")
            {
                /// MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
                wipePasswordFromArgument(*storage.engine, data, 4);
            }
            else if (engine_name == "MongoDB")
            {
                /// MongoDB('host:port', 'database', 'collection', 'user', 'password', ...)
                wipePasswordFromArgument(*storage.engine, data, 4);
            }
            else if (engine_name == "S3" || engine_name == "COSN")
            {
                /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
                wipePasswordFromS3TableEngineArguments(*storage.engine, data);
            }
        }

        static void wipePasswordFromS3TableEngineArguments(ASTFunction & engine, Data & data)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }

            /// We replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
            /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
            /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

            /// But we should check the number of arguments first because we don't need to do that replacements in case of
            /// S3('url' [, 'format' [, 'compression']])
            size_t num_arguments;
            if (!tryGetNumArguments(engine, &num_arguments) || (num_arguments < 4))
                return;

            wipePasswordFromArgument(engine, data, 2);
        }

        static void visitDatabaseEngine(ASTStorage & storage, Data & data)
        {
            if (!storage.engine)
                return;

            const String & engine_name = storage.engine->name;

            if (engine_name == "MySQL" || engine_name == "MaterializeMySQL" || engine_name == "MaterializedMySQL")
            {
                /// MySQL('host:port', 'database', 'user', 'password')
                wipePasswordFromArgument(*storage.engine, data, 3);
            }
            else if (engine_name == "PostgreSQL" || engine_name == "MaterializedPostgreSQL")
            {
                /// PostgreSQL('host:port', 'database', 'user', 'password', ...)
                wipePasswordFromArgument(*storage.engine, data, 3);
            }
        }

        static void visitFunction(ASTFunction & function, Data & data)
        {
            if (function.name == "mysql")
            {
                /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
                wipePasswordFromArgument(function, data, 4);
            }
            else if (function.name == "postgresql")
            {
                /// postgresql('host:port', 'database', 'table', 'user', 'password', ...)
                wipePasswordFromArgument(function, data, 4);
            }
            else if (function.name == "mongodb")
            {
                /// mongodb('host:port', 'database', 'collection', 'user', 'password', ...)
                wipePasswordFromArgument(function, data, 4);
            }
            else if (function.name == "s3" || function.name == "cosn")
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                wipePasswordFromS3FunctionArguments(function, data, /* is_cluster_function= */ false);
            }
            else if (function.name == "s3Cluster")
            {
                /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                wipePasswordFromS3FunctionArguments(function, data, /* is_cluster_function= */ true);
            }
            else if (function.name == "remote" || function.name == "remoteSecure")
            {
                /// remote('addresses_expr', 'db', 'table', 'user', 'password', ...)
                wipePasswordFromRemoteFunctionArguments(function, data);
            }
            else if (
                function.name == "encrypt" || function.name == "decrypt" || function.name == "aes_encrypt_mysql"
                || function.name == "aes_decrypt_mysql" || function.name == "tryDecrypt")
            {
                /// encrypt('mode', 'plaintext', 'key' [, iv, aad])
                wipePasswordFromEncryptionFunctionArguments(function, data);
            }
        }

        static void wipePasswordFromS3FunctionArguments(ASTFunction & function, Data & data, bool is_cluster_function)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }

            /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
            size_t url_arg_idx = is_cluster_function ? 1 : 0;

            /// We're going to replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

            /// But we should check the number of arguments first because we don't need to do any replacements in case of
            /// s3('url' [, 'format']) or s3Cluster('cluster_name', 'url' [, 'format'])
            size_t num_arguments;
            if (!tryGetNumArguments(function, &num_arguments) || (num_arguments < url_arg_idx + 3))
                return;

            if (num_arguments >= url_arg_idx + 5)
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'structure', ...)
                wipePasswordFromArgument(function, data, url_arg_idx + 2);
            }
            else
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                /// We need to distinguish that from s3('url', 'format', 'structure' [, 'compression_method']).
                /// So we will check whether the argument after 'url' is a format.
                String format;
                if (!tryGetEvaluatedConstStringFromArgument(function, url_arg_idx + 1, data.context, &format))
                    return;

                if (FormatFactory::instance().getAllFormats().contains(format))
                    return; /// The argument after 'url' is a format: s3('url', 'format', ...)

                /// The argument after 'url' is not a format so we do our replacement:
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...) -> s3('url', 'aws_access_key_id', '[HIDDEN]', ...)
                wipePasswordFromArgument(function, data, url_arg_idx + 2);
            }
        }

        static void wipePasswordFromRemoteFunctionArguments(ASTFunction & function, Data & data)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }

            /// We're going to replace 'password' with '[HIDDEN'] for the following signatures:
            /// remote('addresses_expr', db.table, 'user' [, 'password'] [, sharding_key])
            /// remote('addresses_expr', 'db', 'table', 'user' [, 'password'] [, sharding_key])
            /// remote('addresses_expr', table_function(), 'user' [, 'password'] [, sharding_key])

            /// But we should check the number of arguments first because we don't need to do any replacements in case of
            /// remote('addresses_expr', db.table)
            size_t num_arguments;
            if (!tryGetNumArguments(function, &num_arguments) || (num_arguments < 3))
                return;

            auto & arguments = function.arguments->as<ASTExpressionList>()->children;
            size_t arg_num = 1;

            /// Skip 1 or 2 arguments with table_function() or db.table or 'db', 'table'.
            const auto * table_function = arguments[arg_num]->as<ASTFunction>();
            if (table_function && TableFunctionFactory::instance().isTableFunctionName(table_function->name))
            {
                ++arg_num;
            }
            else
            {
                String database;
                if (!tryGetEvaluatedConstDatabaseNameFromArgument(function, arg_num, data.context, &database))
                    return;
                ++arg_num;

                auto qualified_name = QualifiedTableName::parseFromString(database);
                if (qualified_name.database.empty())
                    ++arg_num; /// skip 'table' argument
            }

            /// Check if username and password are specified
            /// (sharding_key can be of any type so while we're getting string literals they're username & password).
            String username, password;
            bool username_specified = tryGetStringFromArgument(function, arg_num, &username);
            bool password_specified = username_specified && tryGetStringFromArgument(function, arg_num + 1, &password);

            if (password_specified)
            {
                /// Password is specified so we do our replacement:
                /// remote('addresses_expr', db.table, 'user', 'password', ...) -> remote('addresses_expr', db.table, 'user', '[HIDDEN]', ...)
                wipePasswordFromArgument(function, data, arg_num + 1);
            }
        }

        static void wipePasswordFromEncryptionFunctionArguments(ASTFunction & function, Data & data)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }

            /// We replace all arguments after 'mode' with '[HIDDEN]':
            /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')

            size_t num_arguments;
            if (!tryGetNumArguments(function, &num_arguments) || (num_arguments < 2))
                return;

            wipePasswordFromArgument(function, data, 1);
            function.arguments->as<ASTExpressionList>()->children.resize(2);
        }

        static void visitBackupQuery(ASTBackupQuery & query, Data & data)
        {
            if (query.backup_name)
            {
                if (auto * backup_engine = query.backup_name->as<ASTFunction>())
                    wipePasswordFromBackupEngineArguments(*backup_engine, data);
            }

            if (query.base_backup_name)
            {
                if (auto * backup_engine = query.base_backup_name->as<ASTFunction>())
                    wipePasswordFromBackupEngineArguments(*backup_engine, data);
            }
        }

        static void wipePasswordFromBackupEngineArguments(ASTFunction & engine, Data & data)
        {
            if (engine.name == "S3")
            {
                /// BACKUP ... TO S3(url, [aws_access_key_id, aws_secret_access_key])
                wipePasswordFromArgument(engine, data, 2);
            }
        }

        static void wipePasswordFromArgument(ASTFunction & function, Data & data, size_t arg_idx)
        {
            if constexpr (check_only)
            {
                data.can_contain_password = true;
                return;
            }

            if (!function.arguments)
                return;

            auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return;

            auto & arguments = expr_list->children;

            if (arg_idx < arguments.size())
                arguments[arg_idx] = std::make_shared<ASTLiteral>("[HIDDEN]");
        }

        static bool tryGetNumArguments(const ASTFunction & function, size_t * num_arguments)
        {
            if (!function.arguments)
                return false;

            auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return false;

            *num_arguments = expr_list->children.size();
            return true;
        }

        static bool tryGetStringFromArgument(const ASTFunction & function, size_t arg_idx, String * value)
        {
            if (!function.arguments)
                return false;

            const auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return false;

            const auto & arguments = expr_list->children;

            if (arg_idx >= arguments.size())
                return false;

            const auto * literal = arguments[arg_idx]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::String)
                return false;

            *value = literal->value.safeGet<String>();
            return true;
        }

        static bool
        tryGetEvaluatedConstStringFromArgument(const ASTFunction & function, size_t arg_idx, const ContextPtr & context, String * value)
        {
            if (!function.arguments)
                return false;

            const auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return false;

            const auto & arguments = expr_list->children;

            if (arg_idx >= arguments.size())
                return false;

            ASTPtr argument = arguments[arg_idx];
            try
            {
                argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
            }
            catch (...)
            {
                return false;
            }

            const auto * literal = argument->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::String)
                return false;

            *value = literal->value.safeGet<String>();
            return true;
        }

        static bool tryGetEvaluatedConstDatabaseNameFromArgument(
            const ASTFunction & function, size_t arg_idx, const ContextPtr & context, String * value)
        {
            if (!function.arguments)
                return false;

            const auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return false;

            const auto & arguments = expr_list->children;

            if (arg_idx >= arguments.size())
                return false;

            ASTPtr argument = arguments[arg_idx];
            try
            {
                argument = evaluateConstantExpressionForDatabaseName(argument, context);
            }
            catch (...)
            {
                return false;
            }

            const auto * literal = argument->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::String)
                return false;

            *value = literal->value.safeGet<String>();
            return true;
        }

        static void visitDictionaryDef(ASTDictionary & dictionary, Data & data)
        {
            if (!dictionary.source || !dictionary.source->elements)
                return;

            const auto * elements = dictionary.source->elements->as<ASTExpressionList>();
            if (!elements)
                return;

            /// We replace password in the dictionary's definition:
            /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password 'qwe123' db 'default' table 'ids')) ->
            /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
            for (const auto & element : elements->children)
            {
                auto * pair = element->as<ASTPair>();
                if (!pair)
                    continue;

                if (pair->first == "password")
                {
                    if constexpr (check_only)
                    {
                        data.can_contain_password = true;
                        return;
                    }
                    pair->set(pair->second, std::make_shared<ASTLiteral>("[HIDDEN]"));
                }
            }
        }
    };

    /// Checks the type of a specified AST and returns true if it can contain a password.
    bool canContainPassword(const IAST & ast, PasswordWipingMode mode)
    {
        using WipingVisitor = PasswordWipingVisitor</*check_only= */ true>;
        WipingVisitor::Data data;
        data.mode = mode;
        WipingVisitor::Visitor visitor{data};
        ASTPtr ast_ptr = std::const_pointer_cast<IAST>(ast.shared_from_this());
        visitor.visit(ast_ptr);
        return data.can_contain_password;
    }

    /// Removes a password or its hash from a query if it's specified there or replaces it with some placeholder.
    /// This function is used to prepare a query for storing in logs (we don't want logs to contain sensitive information).
    void wipePasswordFromQuery(ASTPtr ast, PasswordWipingMode mode, const ContextPtr & context)
    {
        using WipingVisitor = PasswordWipingVisitor</*check_only= */ false>;
        WipingVisitor::Data data;
        data.context = context;
        data.mode = mode;
        WipingVisitor::Visitor visitor{data};
        visitor.visit(ast);
    }

    /// Common utility for masking sensitive information.
    String maskSensitiveInfoImpl(const String & query, const ASTPtr & parsed_query, PasswordWipingMode mode, const ContextPtr & context)
    {
        String res = query;

        // Wiping a password or hash from the query because we don't want it to go to logs.
        if (parsed_query && canContainPassword(*parsed_query, mode))
        {
            ASTPtr ast_without_password = parsed_query->clone();
            wipePasswordFromQuery(ast_without_password, mode, context);
            res = serializeAST(*ast_without_password);
        }

        // Wiping sensitive data before cropping query by log_queries_cut_to_length,
        // otherwise something like credit card without last digit can go to log.
        if (auto * masker = SensitiveDataMasker::getInstance())
        {
            auto matches = masker->wipeSensitiveData(res);
            if (matches > 0)
            {
                ProfileEvents::increment(ProfileEvents::QueryMaskingRulesMatch, matches);
            }
        }

        res = res.substr(0, context->getSettingsRef().log_queries_cut_to_length);

        return res;
    }
}


String maskSensitiveInfoInQueryForLogging(const String & query, const ASTPtr & parsed_query, const ContextPtr & context)
{
    return maskSensitiveInfoImpl(query, parsed_query, PasswordWipingMode::Query, context);
}


String maskSensitiveInfoInBackupNameForLogging(const String & backup_name, const ASTPtr & ast, const ContextPtr & context)
{
    return maskSensitiveInfoImpl(backup_name, ast, PasswordWipingMode::BackupName, context);
}

}
