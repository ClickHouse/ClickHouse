#include "config.h"

#if USE_AWS_S3

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseS3.h>

#include <Common/Logger.h>
#include <Common/RemoteHostFilter.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/S3/URI.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/IStorage.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "url",
    "access_key_id",
    "secret_access_key",
    "no_sign_request",
    "use_environment_credentials"
};

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNACCEPTABLE_URL;
    extern const int S3_ERROR;

    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DatabaseS3::DatabaseS3(const String & name_, const Configuration& config_, ContextPtr context_)
    : IDatabase(name_)
    , WithContext(context_->getGlobalContext())
    , config(config_)
    , log(getLogger("DatabaseS3(" + name_ + ")"))
{
}

std::string DatabaseS3::getFullUrl(const std::string & name) const
{
    if (!config.url_prefix.empty())
        return fs::path(config.url_prefix) / name;

    return name;
}

bool DatabaseS3::checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const
{
    try
    {
        S3::URI uri(url);
        context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri.uri);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        return false;
    }
    return true;
}

bool DatabaseS3::isTableExist(const String & name, ContextPtr context_) const
{
    std::lock_guard lock(mutex);
    if (loaded_tables.contains(name))
        return true;

    return checkUrl(getFullUrl(name), context_, false);
}

StoragePtr DatabaseS3::getTableImpl(const String & name, ContextPtr context_) const
{
    /// Not cached across sessions: the built S3 client depends on the per-session
    /// `s3_allow_server_credentials_in_user_queries`, so reuse could let an allowed session prime a client for a
    /// restricted one. Rebuild every time. Caching can return once it keys on the credential mode.
    auto url = getFullUrl(name);
    checkUrl(url, context_, /* throw_on_error */true);

    auto function = make_intrusive<ASTFunction>();
    function->name = "s3";
    function->arguments = make_intrusive<ASTExpressionList>();
    function->children.push_back(function->arguments);

    function->arguments->children.push_back(make_intrusive<ASTLiteral>(url));
    if (config.no_sign_request)
    {
        function->arguments->children.push_back(make_intrusive<ASTLiteral>("NOSIGN"));
    }
    else if (config.access_key_id.has_value() && config.secret_access_key.has_value())
    {
        function->arguments->children.push_back(make_intrusive<ASTLiteral>(config.access_key_id.value()));
        function->arguments->children.push_back(make_intrusive<ASTLiteral>(config.secret_access_key.value()));
    }
    else if (config.use_environment_credentials)
    {
        /// Carry the opt-in as an explicit key-value arg; the positional `s3(url)` form would lose it. The
        /// table function then applies the user-query credential restriction like any other query.
        function->arguments->children.push_back(
            makeASTFunction("equals",
                make_intrusive<ASTIdentifier>("use_environment_credentials"),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(1))));
    }

    auto table_function = TableFunctionFactory::instance().get(function, context_);
    if (!table_function)
        return nullptr;

    /// TableFunctionS3 throws exceptions, if table cannot be created.
    auto table_storage = table_function->execute(function, context_, name, /*cached_columns_=*/{}, /*use_global_context=*/false, /*is_insert_query=*/true);
    /// Intentionally not cached -- see the note above.

    return table_storage;
}

StoragePtr DatabaseS3::getTable(const String & name, ContextPtr context_) const
{
    /// Rethrow all exceptions from TableFunctionS3 to show correct error to user.
    if (auto storage = getTableImpl(name, context_))
        return storage;

    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseS3::tryGetTable(const String & name, ContextPtr context_) const
{
    try
    {
        return getTableImpl(name, context_);
    }
    catch (const Exception & e)
    {
        /// Ignore exceptions thrown by TableFunctionS3, which indicate that there is no table.
        if (e.code() == ErrorCodes::BAD_ARGUMENTS
            || e.code() == ErrorCodes::S3_ERROR
            || e.code() == ErrorCodes::FILE_DOESNT_EXIST
            || e.code() == ErrorCodes::UNACCEPTABLE_URL)
        {
            return nullptr;
        }
        throw;
    }
    catch (const Poco::URISyntaxException &)
    {
        return nullptr;
    }
}

bool DatabaseS3::empty() const
{
    std::lock_guard lock(mutex);
    return loaded_tables.empty();
}

ASTPtr DatabaseS3::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    std::string creation_args;
    creation_args += fmt::format("'{}'", config.url_prefix);
    if (config.no_sign_request)
        creation_args += ", 'NOSIGN'";
    else if (config.access_key_id.has_value() && config.secret_access_key.has_value())
        creation_args += fmt::format(", '{}', '{}'", config.access_key_id.value(), config.secret_access_key.value());
    else if (config.use_environment_credentials)
        creation_args += ", use_environment_credentials = 1";

    const String query = fmt::format("CREATE DATABASE {} ENGINE = S3({})", backQuoteIfNeed(database_name), creation_args);
    ASTPtr ast
        = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
    }

    return ast;
}

void DatabaseS3::shutdown()
{
    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = loaded_tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->flushAndShutdown();
    }

    std::lock_guard lock(mutex);
    loaded_tables.clear();
}

DatabaseS3::Configuration DatabaseS3::parseArguments(ASTs engine_args, ContextPtr context_)
{
    Configuration result;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context_))
    {
        auto & collection = *named_collection;

        validateNamedCollection(collection, {}, optional_configuration_keys);

        result.url_prefix = collection.getOrDefault<String>("url", "");
        result.no_sign_request = collection.getOrDefault<bool>("no_sign_request", false);
        result.use_environment_credentials = collection.getOrDefault<bool>("use_environment_credentials", false);

        auto key_id = collection.getOrDefault<String>("access_key_id", "");
        auto secret_key = collection.getOrDefault<String>("secret_access_key", "");

        if (!key_id.empty())
            result.access_key_id = key_id;

        if (!secret_key.empty())
            result.secret_access_key = secret_key;

        /// A URL-only collection is anonymous; record it as NOSIGN so `getTableImpl` does not flatten it to
        /// the positional `s3(url)` form, whose built-in `use_environment_credentials = 1` default would
        /// resolve the server's credentials.
        const bool has_complete_keys = result.access_key_id.has_value() && result.secret_access_key.has_value();
        if (!result.no_sign_request && !has_complete_keys
            && !collection.getOrDefault<bool>("use_environment_credentials", false))
        {
            result.no_sign_request = true;
        }
    }
    else
    {
        const std::string supported_signature =
            " - S3()\n"
            " - S3('url')\n"
            " - S3('url', 'NOSIGN')\n"
            " - S3('url', 'access_key_id', 'secret_access_key')\n";
        const auto error_message =
            fmt::format("Engine DatabaseS3 must have the following arguments signature\n{}", supported_signature);

        /// Accept a `use_environment_credentials = N` key-value argument (emitted by
        /// `getCreateDatabaseQueryImpl` for a named-collection database that opted in) so that form round-trips.
        auto is_use_environment_credentials_kv = [&](const ASTPtr & arg)
        {
            const auto * func = arg->as<ASTFunction>();
            if (!func || func->name != "equals" || !func->arguments || func->arguments->children.size() != 2)
                return false;
            const auto * key = func->arguments->children[0]->as<ASTIdentifier>();
            if (!key || key->name() != "use_environment_credentials")
                return false;
            const auto * value = func->arguments->children[1]->as<ASTLiteral>();
            if (!value)
                return false;
            result.use_environment_credentials = value->value.safeGet<UInt64>() != 0;
            return true;
        };
        engine_args.erase(
            std::remove_if(engine_args.begin(), engine_args.end(), is_use_environment_credentials_kv),
            engine_args.end());

        for (auto & arg : engine_args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

        if (engine_args.size() > 3)
            throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message.c_str());

        if (engine_args.empty())
            return result;

        result.url_prefix = checkAndGetLiteralArgument<String>(engine_args[0], "url");

        // url, NOSIGN
        if (engine_args.size() == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                result.no_sign_request = true;
            else
                throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());
        }

        // url, access_key_id, secret_access_key
        if (engine_args.size() == 3)
        {
            auto key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            auto secret_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (key_id.empty() || secret_key.empty() || boost::iequals(key_id, "NOSIGN"))
                throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());

            result.access_key_id = key_id;
            result.secret_access_key = secret_key;
        }
    }

    return result;
}

/**
 * Returns an empty vector because the database is read-only and no tables can be backed up
 */
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseS3::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access
 */
DatabaseTablesIteratorPtr DatabaseS3::getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void registerDatabaseS3(DatabaseFactory & factory);
void registerDatabaseS3(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        DatabaseS3::Configuration config;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            ASTs & engine_args = engine->arguments->children;
            config = DatabaseS3::parseArguments(engine_args, args.context);
        }

        return std::make_shared<DatabaseS3>(args.database_name, config, args.context);
    };
    factory.registerDatabase("S3", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::S3,
    }, Documentation{
        .description = "A read-only database that exposes objects in Amazon S3 (or S3-compatible storage) as tables.",
        .syntax = "ENGINE = S3([config_or_url[, access_key_id, secret_access_key]])",
        .related = {"Filesystem", "HDFS"}});
}
}
#endif
