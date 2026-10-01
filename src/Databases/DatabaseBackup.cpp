#include <Databases/DatabaseBackup.h>

#include <filesystem>

#include <boost/algorithm/string.hpp>

#include <Poco/Util/LayeredConfiguration.h>

#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>

#include <IO/ReadBufferFromFileBase.h>

#include <Core/Settings.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>

#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/IStorage.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Backups/BackupFactory.h>

#include <Disks/DiskBackup.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/TablesLoader.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/LoadingStrictnessLevel.h>


namespace CurrentMetrics
{
    extern const Metric DatabaseBackupThreads;
    extern const Metric DatabaseBackupThreadsActive;
    extern const Metric DatabaseBackupThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_FILE_NAME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}

namespace
{

class DiskBackupConfiguration : public Poco::Util::LayeredConfiguration
{
public:
    DiskBackupConfiguration()
        : Poco::Util::LayeredConfiguration()
    {}
};

String buildStoragePolicyName(const DatabaseBackup::Configuration & config)
{
    return fmt::format("__database_backup_config_{}_{})", config.database_name, config.backup_info.toString());
}

void updateCreateQueryWithDatabaseBackupStoragePolicy(ASTCreateQuery * create_query, const DatabaseBackup::Configuration & config, ContextPtr)
{
    auto * storage = create_query->storage;

    bool is_replicated_or_shared_engine = false;
    auto engine = make_intrusive<ASTFunction>();

    static constexpr std::string_view replicated_engine_prefix = "Replicated";

    static constexpr std::string_view shared_engine_prefix = "Shared";

    if (storage->engine->name.starts_with(replicated_engine_prefix))
    {
        is_replicated_or_shared_engine = true;
        engine->name = storage->engine->name.substr(replicated_engine_prefix.size());
    }
    else if (storage->engine->name.starts_with(shared_engine_prefix))
    {
        is_replicated_or_shared_engine = true;
        engine->name = storage->engine->name.substr(shared_engine_prefix.size());
    }
    else
    {
        engine->name = storage->engine->name;
    }

    /// Add old engine's arguments
    auto args = make_intrusive<ASTExpressionList>();

    if (storage->engine->arguments)
    {
        /// Ignore first two arguments for replicated engine
        size_t i = is_replicated_or_shared_engine ? 2 : 0;
        for (; i < storage->engine->arguments->children.size(); ++i)
            args->children.push_back(storage->engine->arguments->children[i]->clone());
    }

    engine->arguments = std::move(args);
    engine->setNoEmptyArgs(true);

    /// Set new engine for the old query
    create_query->storage->set(create_query->storage->engine, engine->clone());

    ASTSetQuery * settings = storage->settings->as<ASTSetQuery>();
    bool has_settings = settings != nullptr;

    if (has_settings)
    {
        while (settings->changes.removeSetting("disk"))
            ;

        while (settings->changes.removeSetting("storage_policy"))
            ;
    }
    else
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        storage->set(storage->settings, settings_ast);
        settings = storage->settings;
    }

    auto storage_policy_name = buildStoragePolicyName(config);
    settings->changes.setSetting("storage_policy", Field(storage_policy_name));
}

}

DatabaseBackup::DatabaseBackup(const String & name_, const String & metadata_path_, const Configuration & config_, ContextPtr context_)
    : DatabaseOrdinary(
        name_,
        metadata_path_,
        DatabaseCatalog::getDataDirPath(name_) / "",
        "DatabaseBackup(" + name_ + ")",
        context_)
    , config(config_)
{
}

void DatabaseBackup::createTable(
    ContextPtr,
    const String &,
    const StoragePtr &,
    const ASTPtr &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CREATE TABLE is not supported for Backup database");
}

StoragePtr DatabaseBackup::detachTable(ContextPtr, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DETACH TABLE is not supported for Backup database");
}

void DatabaseBackup::detachTablePermanently(ContextPtr, const String &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DETACH TABLE is not supported for Backup database");
}

void DatabaseBackup::dropTable(
    ContextPtr,
    const String &,
    bool)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "DROP TABLE is not supported for Backup database");
}

void DatabaseBackup::renameTable(
    ContextPtr,
    const String &,
    IDatabase &,
    const String &,
    bool,
    bool)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "RENAME TABLE is not supported for Backup database");
}

void DatabaseBackup::alterTable(ContextPtr, const StorageID &, const StorageInMemoryMetadata &, const bool)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ALTER TABLE is not supported for Backup database");
}

void DatabaseBackup::beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel mode)
{
    DatabaseOrdinary::beforeLoadingMetadata(local_context, mode);

    BackupFactory::CreateParams backup_open_params;
    backup_open_params.open_mode = IBackup::OpenMode::READ;
    backup_open_params.context = getContext();
    backup_open_params.backup_info = config.backup_info;

    backup = BackupFactory::instance().createBackup(backup_open_params);

    auto storage_policy_name = buildStoragePolicyName(config);

    getContext()->getOrCreateStoragePolicy(storage_policy_name, [&](const StoragePoliciesMap &)
    {
        DiskBackup::PathPrefixReplacement path_prefix_replacement;
        path_prefix_replacement.from = data_path;
        path_prefix_replacement.to = DatabaseCatalog::getDataDirPath(config.database_name) / "";

        DiskBackupConfiguration disk_backup_config;

        auto disk = std::make_shared<DiskBackup>(backup, std::move(path_prefix_replacement), disk_backup_config, "");
        auto volume = std::make_shared<SingleDiskVolume>("_volume_backup_disk", disk);

        static constexpr double move_factor_for_single_disk_volume = 0.0;
        return std::make_shared<StoragePolicy>(storage_policy_name, Volumes{volume}, move_factor_for_single_disk_volume);
    });
}

void DatabaseBackup::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool)
{
    if (!backup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not initialized");

    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    std::vector<std::string> metadata_files;

    auto tables_files = backup->listFiles("/metadata/" + config.database_name, false /*recursive*/);

    for (const auto & file_name : tables_files)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (file_name.at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(file_name, ".sql.bak"))
        {
            continue;
        }

        /// Permanently detached table flag
        if (endsWith(file_name, ".sql.detached"))
        {
            continue;
        }

        if (endsWith(file_name, ".sql.tmp_drop"))
        {
            /// There are files that we tried to delete previously
            continue;
        }
        else if (endsWith(file_name, ".sql.tmp"))
        {
            /// There are files .sql.tmp
            continue;
        }
        else if (endsWith(file_name, ".sql"))
        {
            /// The required files have names like `table_name.sql`
            metadata_files.emplace_back(file_name);
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Incorrect file extension: {} in metadata directory {}", file_name, getMetadataPath());
        }
    }

    /// NOTE No concurrent writes are possible during database loading
    const std::string current_database_name = TSA_SUPPRESS_WARNING_FOR_READ(database_name);

    auto process_metadata_file = [&metadata, &current_database_name, local_context, this](const String & file_name)
    {
        std::filesystem::path metadata_path("/metadata/" + config.database_name);
        std::filesystem::path metadata_file_path = metadata_path / file_name;

        try
        {
            auto buffer = backup->readFile(metadata_file_path.string());

            String query;
            readStringUntilEOF(query, *buffer);

            /// We do not pass logger here, because we do not want to log warnings about backup metadata files
            /// containing both UUID and table name, this is expected
            auto ast = DatabaseOnDisk::parseQueryFromMetadata({}, local_context, metadata_file_path.string(), query);
            if (!ast)
                return;

            FunctionNameNormalizer::visit(ast.get());

            auto * create_query = ast->as<ASTCreateQuery>();

            create_query->attach = true;
            create_query->setDatabase(current_database_name);
            create_query->uuid = UUIDHelpers::Nil;

            if (!create_query->storage)
            {
                LOG_INFO(log, "Skipping table {} with no storage.", create_query->getTable());
                return;
            }

            if (!create_query->storage->engine->name.ends_with("MergeTree"))
            {
                LOG_INFO(log, "Skipping table {} with engine {}. Only MergeTree engine is supported.",
                    create_query->getTable(), create_query->storage->engine->name);
                return;
            }

            updateCreateQueryWithDatabaseBackupStoragePolicy(create_query, config, local_context);

            NormalizeSelectWithUnionQueryVisitor::Data data{local_context->getSettingsRef()[Setting::union_default_mode]};
            NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);

            QualifiedTableName qualified_name{current_database_name, create_query->getTable()};

            {
                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{metadata_file_path.string(), ast};
                metadata.total_dictionaries += create_query->is_dictionary;
            }

            {
                std::lock_guard lock{table_name_to_create_query_mutex};
                table_name_to_create_query[create_query->getTable()] = ast;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + metadata_file_path.string());
            throw;
        }
    };

    std::sort(metadata_files.begin(), metadata_files.end());
    metadata_files.erase(std::unique(metadata_files.begin(), metadata_files.end()), metadata_files.end());

    /// Read and parse metadata in parallel
    ThreadPool pool(CurrentMetrics::DatabaseBackupThreads, CurrentMetrics::DatabaseBackupThreadsActive, CurrentMetrics::DatabaseBackupThreadsScheduled);

    {
        /// Note that we pass batch by value (always fine) and process_metadata_file by reference
        /// process_metadata_file is ok since a) it outlives runner and b) it captures by reference only things that outlive runner
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::DATABASE_BACKUP);
        const auto batch_size = metadata_files.size() / pool.getMaxThreads() + 1;

        for (auto it = metadata_files.begin(); it < metadata_files.end(); std::advance(it, batch_size))
        {
            std::span batch{it, std::min(std::next(it, batch_size), metadata_files.end())};
            runner.enqueueAndKeepTrack([batch, &process_metadata_file]() mutable
                {
                    for (const auto & file : batch)
                        process_metadata_file(file);
                },
                Priority{},
                getContext()->getSettingsRef()[Setting::lock_acquire_timeout].totalMicroseconds());
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "Metadata processed, database {} has {} tables and {} dictionaries in total.",
        current_database_name, tables_in_database, dictionaries_in_database);
}

ASTPtr DatabaseBackup::getCreateQueryFromMetadata(const String & table_name, bool throw_on_error) const
{
    std::lock_guard lock{table_name_to_create_query_mutex};

    auto it = table_name_to_create_query.find(table_name);
    if (it == table_name_to_create_query.end())
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Table {} does not exist", table_name);

        return {};
    }

    auto create_query = it->second->clone();

    auto & create_query_typed = create_query->as<ASTCreateQuery &>();
    create_query_typed.attach = false;
    create_query_typed.setDatabase(getDatabaseName());

    return create_query;
}

ASTPtr DatabaseBackup::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();

    /// The locator is emitted as the function it is, not as a string literal holding its text: this
    /// definition is what `ALTER DATABASE ... MODIFY COMMENT` writes back into the metadata file, and
    /// the load path parses the second argument with `BackupInfo::fromAST`, which takes a function.
    const String query = fmt::format("CREATE DATABASE {} ENGINE = Backup({}, {})",
        backQuoteIfNeed(database_name), quoteString(config.database_name), config.backup_info.toString());

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser,
        query.data(),
        query.data() + query.size(),
        "",
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
    }

    return ast;
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseBackup::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

namespace
{

DatabaseBackup::Configuration parseArguments(ASTs engine_args, ContextPtr, bool allow_locator_in_string_literal)
{
    if (engine_args.size() != 2)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "DatabaseBackup must have 2 arguments: database_name, backup_configuration");

    DatabaseBackup::Configuration result;

    result.database_name = checkAndGetLiteralArgument<String>(engine_args[0], "database_name");

    /** A locator held in a string literal (`Backup('db', 'File(\'backup.zip\')')`) is the form that
      * metadata rewritten by an older server carries, so it has to keep loading - a server that cannot
      * parse its own metadata does not start at all.
      *
      * Only there: in a statement a user writes the locator must be the function it is, because that is
      * the form `FunctionSecretArgumentsFinder` knows how to redact, and a quoted one would carry its
      * credentials verbatim into `query_log`, `SHOW PROCESSLIST` and the distributed DDL payload.
      */
    if (allow_locator_in_string_literal)
    {
        if (const auto * locator = engine_args[1]->as<ASTLiteral>(); locator && locator->value.getType() == Field::Types::String)
        {
            result.backup_info = BackupInfo::fromString(locator->value.safeGet<String>());
            return result;
        }
    }

    /// `BackupInfo::fromAST` puts the offending argument into its message, and that message reaches the
    /// error log and the `exception` column of `query_log`. A locator held in a string literal is exactly
    /// the text that can carry credentials, so refuse it here without echoing it.
    if (!engine_args[1]->as<ASTFunction>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected function as the backup destination of a `Backup` database. It must be spelled as the "
            "function it is, such as `File('backup')` or `S3(...)`. The text given is not shown, because a "
            "destination held in a string literal cannot be redacted and may carry credentials");

    result.backup_info = BackupInfo::fromAST(*engine_args[1]);

    return result;
}

}

void registerDatabaseBackup(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        ASTs engine_args;
        if (engine->arguments)
            engine_args = engine->arguments->children;

        /** Whether the locator may be held in a string literal. Metadata an older server rewrote carries that
          * form and has to keep loading, but a statement a user writes must spell the locator as the function it
          * is, because that is the form `FunctionSecretArgumentsFinder` can redact.
          *
          * Metadata is read back on three paths: the short `ATTACH DATABASE db`, a load under `force_restore_data`,
          * and the replay of the stored full `ATTACH DATABASE db ENGINE = Backup(...)` statement at server start.
          * The last one runs in plain `ATTACH` mode, so neither of the first two conditions covers it - and it is
          * exactly the path that has to load metadata an older server rewrote.
          *
          * The loader flag, not `internal`, is the discriminator: wrappers such as `PARALLEL WITH` run user
          * statements as internal ones, and a user's `ATTACH DATABASE ... ENGINE = Backup(...)` must not get its
          * locator accepted in the form the secret masker cannot redact.
          */
        const bool is_internal_metadata_replay = args.is_metadata_replay && args.mode >= LoadingStrictnessLevel::ATTACH;
        const bool from_existing_metadata
            = isLoadingFromExistingMetadata(args.mode) || args.create_query.attach_short_syntax || is_internal_metadata_replay;

        auto config = parseArguments(engine_args, args.context, /*allow_locator_in_string_literal=*/ from_existing_metadata);

        return std::make_shared<DatabaseBackup>(args.database_name, args.metadata_path, config, args.context);
    };

    factory.registerDatabase("Backup", create_fn, {.supports_arguments = true});
}

}
