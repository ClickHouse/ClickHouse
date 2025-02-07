#include <filesystem>
#include <memory>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TableNameHints.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamePrompter.h>
#include <Common/quoteString.h>


namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric AttachedDatabase;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int THERE_IS_NO_QUERY;
}

namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;

    TableNameHints hints(this->shared_from_this(), context);
    /// hint is a pair which holds a single database_name and table_name suggestion for the given table name.
    auto hint = hints.getHintForTable(name);

    if (hint.first.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
    throw Exception(
        ErrorCodes::UNKNOWN_TABLE,
        "Table {}.{} does not exist. Maybe you meant {}.{}?",
        backQuoteIfNeed(getDatabaseName()),
        backQuoteIfNeed(name),
        backQuoteIfNeed(hint.first),
        backQuoteIfNeed(hint.second));
}

IDatabase::IDatabase(String database_name_) : database_name(std::move(database_name_))
{
    CurrentMetrics::add(CurrentMetrics::AttachedDatabase, 1);
}

IDatabase::~IDatabase()
{
    CurrentMetrics::sub(CurrentMetrics::AttachedDatabase, 1);
}

std::vector<std::pair<ASTPtr, StoragePtr>> IDatabase::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    /// Cannot backup any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup tables in database {}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()));
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

void IDatabase::alterDatabaseComment(const AlterCommand & command, ContextPtr query_context)
{
    if (!command.comment)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't get comment of database");

    setDatabaseComment(command.comment.value());
    ASTPtr ast = getCreateDatabaseQuery();
    if (!ast)
        throw Exception(ErrorCodes::THERE_IS_NO_QUERY, "Unable to show the create query of database {}", getDatabaseName());

    auto * ast_create_query = ast->as<ASTCreateQuery>();
    if (!ast_create_query->is_dictionary)
        ast_create_query->attach = true;

    WriteBufferFromOwnString statement_buf;
    formatAST(*ast_create_query, statement_buf, false);
    writeChar('\n', statement_buf);
    String statement = statement_buf.str();

    auto database_metadata_tmp_path = fs::path("metadata") / (escapeForFileName(getDatabaseName()) + ".sql.tmp");
    auto database_metadata_path = fs::path("metadata") / (escapeForFileName(getDatabaseName()) + ".sql");
    auto db_disk = query_context->getDatabaseDisk();

    writeMetadataFile(
        db_disk, /*file_path=*/database_metadata_tmp_path, /*content=*/statement, query_context->getSettingsRef()[Setting::fsync_metadata]);

    try
    {
        /// rename atomically replaces the old file with the new one.
        db_disk->replaceFile(database_metadata_tmp_path, database_metadata_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(database_metadata_tmp_path);
        throw;
    }
}


}
