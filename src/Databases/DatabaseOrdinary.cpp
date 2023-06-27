#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/TablesLoader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/AsyncLoaderPoolId.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & metadata_path_, ContextPtr context_)
    : DatabaseOrdinary(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseOrdinary (" + name_ + ")", context_)
{
}

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr context_)
    : DatabaseOnDisk(name_, metadata_path_, data_path_, logger, context_)
{
}

void DatabaseOrdinary::loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel, bool)
{
    // Because it supportsLoadingInTopologicalOrder, we don't need this loading method.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

void DatabaseOrdinary::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto process_metadata = [&metadata, is_startup, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                /// NOTE No concurrent writes are possible during database loading
                create_query->setDatabase(TSA_SUPPRESS_WARNING_FOR_READ(database_name));

                /// Even if we don't load the table we can still mark the uuid of it as taken.
                if (create_query->uuid != UUIDHelpers::Nil)
                {
                    /// A bit tricky way to distinguish ATTACH DATABASE and server startup (actually it's "force_attach" flag).
                    if (is_startup)
                    {
                        /// Server is starting up. Lock UUID used by permanently detached table.
                        DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);
                    }
                    else if (!DatabaseCatalog::instance().hasUUIDMapping(create_query->uuid))
                    {
                        /// It's ATTACH DATABASE. UUID for permanently detached table must be already locked.
                        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
                        if (getEngineName() != "MaterializedPostgreSQL")
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create_query->uuid);
                    }
                }

                if (fs::exists(full_path.string() + detached_suffix))
                {
                    const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));
                    permanently_detached_tables.push_back(table_name);
                    LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));
                    return;
                }

                QualifiedTableName qualified_name{TSA_SUPPRESS_WARNING_FOR_READ(database_name), create_query->getTable()};

                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast};
                metadata.total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(local_context, process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "Metadata processed, database {} has {} tables and {} dictionaries in total.",
             TSA_SUPPRESS_WARNING_FOR_READ(database_name), tables_in_database, dictionaries_in_database);
}

void DatabaseOrdinary::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    assert(name.database == TSA_SUPPRESS_WARNING_FOR_READ(database_name));
    const auto & query = ast->as<const ASTCreateQuery &>();

    try
    {
        auto [table_name, table] = createTableFromAST(
            query,
            name.database,
            getTableDataPath(query),
            local_context,
            LoadingStrictnessLevel::FORCE_RESTORE <= mode);

        attachTable(local_context, table_name, table, getTableDataPath(query));
    }
    catch (Exception & e)
    {
        e.addMessage(
            "Cannot attach table " + backQuote(name.database) + "." + backQuote(query.getTable()) + " from metadata file " + file_path
            + " from query " + serializeAST(query));
        throw;
    }
}

LoadTaskPtr DatabaseOrdinary::loadTableFromMetadataAsync(
    AsyncLoader & async_loader,
    LoadJobSet load_after,
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    std::scoped_lock lock(mutex);
    auto job = makeLoadJob(
        std::move(load_after),
        AsyncLoaderPoolId::BackgroundLoad,
        fmt::format("load table {}", name.getFullName()),
        [this, local_context, file_path, name, ast, mode] (const LoadJobPtr &)
        {
            loadTableFromMetadata(local_context, file_path, name, ast, mode);
        });

    return load_table[name.table] = makeLoadTask(async_loader, {job});
}

LoadTaskPtr DatabaseOrdinary::startupTableAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    const QualifiedTableName & name,
    LoadingStrictnessLevel /*mode*/)
{
    std::scoped_lock lock(mutex);

    /// Initialize progress indication on the first call
    if (total_tables_to_startup == 0)
    {
        total_tables_to_startup = tables.size();
        startup_watch.restart();
    }

    auto job = makeLoadJob(
        std::move(startup_after),
        AsyncLoaderPoolId::BackgroundStartup,
        fmt::format("startup table {}", name.getFullName()),
        [this, name] (const LoadJobPtr &)
        {
            if (auto table = DatabaseOnDisk::tryGetTable(name.table, {}))
            {
                /// Since startup() method can use physical paths on disk we don't allow any exclusive actions (rename, drop so on)
                /// until startup finished.
                auto table_lock_holder = table->lockForShare(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);
                table->startup();
                logAboutProgress(log, ++tables_started, total_tables_to_startup, startup_watch);
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist during startup",
                    backQuote(name.database), backQuote(name.table));
        });

    return startup_table[name.table] = makeLoadTask(async_loader, {job});
}

LoadTaskPtr DatabaseOrdinary::startupDatabaseAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    LoadingStrictnessLevel /*mode*/)
{
    std::scoped_lock lock(mutex);
    // NOTE: this task is empty, but it is required for correct dependency handling (startup should be done after tables loading)
    auto job = makeLoadJob(
        std::move(startup_after),
        AsyncLoaderPoolId::BackgroundStartup,
        fmt::format("startup Ordinary database {}", database_name));
    return makeLoadTask(async_loader, {job});
}

// TODO(serxa): implement
// DatabaseTablesIteratorPtr DatabaseOrdinary::getTablesIterator(ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const
// }

StoragePtr DatabaseOrdinary::tryGetTable(const String & name, ContextPtr local_context) const
{
    const LoadTaskPtr * startup_task = nullptr;
    {
        std::scoped_lock lock(mutex);
        if (auto it = startup_table.find(name); it != startup_table.end())
            startup_task = &it->second;
    }

    // Prioritize jobs (load and startup the table) to be executed in foreground pool and wait for them synchronously
    if (startup_task)
        waitLoad(currentPoolOr(AsyncLoaderPoolId::Foreground), *startup_task);

    return DatabaseOnDisk::tryGetTable(name, local_context);
}

void DatabaseOrdinary::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    String table_name = table_id.table_name;
    /// Read the definition of the table and replace the necessary parts with new ones.
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        statement.data(),
        statement.data() + statement.size(),
        "in file " + table_metadata_path,
        0,
        local_context->getSettingsRef().max_parser_depth);

    applyMetadataChangesToCreateQuery(ast, metadata);

    statement = getObjectDefinitionFromCreateQuery(ast);
    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (local_context->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    /// The create query of the table has been just changed, we need to update dependencies too.
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies, loading_dependencies);

    commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, local_context);
}

void DatabaseOrdinary::commitAlterTable(const StorageID &, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & /*statement*/, ContextPtr /*query_context*/)
{
    try
    {
        /// rename atomically replaces the old file with the new one.
        fs::rename(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        throw;
    }
}

}
