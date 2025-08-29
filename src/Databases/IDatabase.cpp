#include <memory>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TableNameHints.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamePrompter.h>
#include <Common/quoteString.h>


namespace CurrentMetrics
{
    extern const Metric AttachedDatabase;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;

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

[[noreturn]] static void throwAlterDatabaseCommentNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: ALTER DATABASE COMMENT is not supported", engine);
}

[[noreturn]] static void throwBackupTablesNotSupported(const std::string & engine, const std::string & db) {
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup tables in database {}",
                    engine, db);
}

[[noreturn]] static void throwRestoreTablesNotSupported(const std::string & engine, const std::string & db, const std::string & table) {
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    engine, db, table);
}

[[noreturn]] static void throwNotImplemented() {
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

[[noreturn]] static void throwGetDetachedTablesNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get detached tables for Database{}", engine);
}

[[noreturn]] static void throwCreateTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no CREATE TABLE query for Database{}", engine);
}

[[noreturn]] static void throwDropTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DROP TABLE query for Database{}", engine);
}

[[noreturn]] static void throwAttachTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no ATTACH TABLE query for Database{}", engine);
}

[[noreturn]] static void throwDetachTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DETACH TABLE query for Database{}", engine);
}

[[noreturn]] static void throwDetachTablePermanentlyNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DETACH TABLE PERMANENTLY query for Database{}", engine);
}

[[noreturn]] static void throwGetNamesOfPermanentlyDetachedTablesNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get names of permanently detached tables for Database{}", engine);
}

[[noreturn]] static void throwRenameTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: renameTable() is not supported", engine);
}

[[noreturn]] static void throwAlterTableNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: alterTable() is not supported", engine);
}

[[noreturn]] static void throwRenameDatabaseNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: RENAME DATABASE is not supported", engine);
}

[[noreturn]] static void throwApplySettingsChangesNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Database engine {} either does not support settings, or does not support altering settings",
                    engine);
}

[[noreturn]] static void throwStopReplicationNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database engine {} does not run a replication thread", engine);
}

[[noreturn]] static void throwReplicatedDDLNotSupported(const std::string & engine) {
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database engine {} does not have replicated DDL queue", engine);
}

void IDatabase::alterDatabaseComment(const AlterCommand & /*command*/)
{
    throwAlterDatabaseCommentNotSupported(getEngineName());
}

std::vector<std::pair<ASTPtr, StoragePtr>> IDatabase::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    throwBackupTablesNotSupported(getEngineName(), backQuoteIfNeed(getDatabaseName()));
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    throwRestoreTablesNotSupported(getEngineName(), backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

void IDatabase::loadTablesMetadata(ContextPtr /*local_context*/, ParsedTablesMetadata & /*metadata*/, bool /*is_startup*/)
{
    throwNotImplemented();
}

void IDatabase::loadTableFromMetadata(
    ContextMutablePtr /*local_context*/,
    const String & /*file_path*/,
    const QualifiedTableName & /*name*/,
    const ASTPtr & /*ast*/,
    LoadingStrictnessLevel /*mode*/)
{
    throwNotImplemented();
}

LoadTaskPtr IDatabase::loadTableFromMetadataAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*load_after*/,
    ContextMutablePtr /*local_context*/,
    const String & /*file_path*/,
    const QualifiedTableName & /*name*/,
    const ASTPtr & /*ast*/,
    LoadingStrictnessLevel /*mode*/)
{
    throwNotImplemented();
}

LoadTaskPtr IDatabase::startupTableAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*startup_after*/,
    const QualifiedTableName & /*name*/,
    LoadingStrictnessLevel /*mode*/)
{
    throwNotImplemented();
}

LoadTaskPtr IDatabase::startupDatabaseAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*startup_after*/,
    LoadingStrictnessLevel /*mode*/)
{
    throwNotImplemented();
}

DatabaseDetachedTablesSnapshotIteratorPtr IDatabase::getDetachedTablesIterator(
    ContextPtr /*context*/, const FilterByNameFunction & /*filter_by_table_name = {}*/, bool /*skip_not_loaded = false*/) const
{
    throwGetDetachedTablesNotSupported(getEngineName());
}

void IDatabase::createTable(
    ContextPtr /*context*/,
    const String & /*name*/,
    const StoragePtr & /*table*/,
    const ASTPtr & /*query*/)
{
    throwCreateTableNotSupported(getEngineName());
}

void IDatabase::dropTable( /// NOLINT
    ContextPtr /*context*/,
    const String & /*name*/,
    [[maybe_unused]] bool sync)
{
    throwDropTableNotSupported(getEngineName());
}

void IDatabase::attachTable(ContextPtr /* context */, const String & /*name*/, const StoragePtr & /*table*/, [[maybe_unused]] const String & relative_table_path) /// NOLINT
{
    throwAttachTableNotSupported(getEngineName());
}

StoragePtr IDatabase::detachTable(ContextPtr /* context */, const String & /*name*/)
{
    throwDetachTableNotSupported(getEngineName());
}

void IDatabase::detachTablePermanently(ContextPtr /*context*/, const String & /*name*/)
{
    throwDetachTablePermanentlyNotSupported(getEngineName());
}

Strings IDatabase::getNamesOfPermanentlyDetachedTables() const
{
    throwGetNamesOfPermanentlyDetachedTablesNotSupported(getEngineName());
}

void IDatabase::renameTable(
    ContextPtr /*context*/,
    const String & /*name*/,
    IDatabase & /*to_database*/,
    const String & /*to_name*/,
    bool /*exchange*/,
    bool /*dictionary*/)
{
    throwRenameTableNotSupported(getEngineName());
}

void IDatabase::alterTable(
    ContextPtr /*context*/,
    const StorageID & /*table_id*/,
    const StorageInMemoryMetadata & /*metadata*/)
{
    throwAlterTableNotSupported(getEngineName());
}

void IDatabase::renameDatabase(ContextPtr, const String & /*new_name*/)
{
    throwRenameDatabaseNotSupported(getEngineName());
}

void IDatabase::applySettingsChanges(const SettingsChanges &, ContextPtr)
{
    throwApplySettingsChangesNotSupported(getEngineName());
}

void IDatabase::stopReplication()
{
    throwStopReplicationNotSupported(getEngineName());
}

BlockIO IDatabase::tryEnqueueReplicatedDDL(const ASTPtr & /*query*/, ContextPtr /*query_context*/, [[maybe_unused]] QueryFlags flags) /// NOLINT
{
    throwReplicatedDDLNotSupported(getEngineName());
}

ASTPtr IDatabase::getCreateTableQueryImpl(const String & /*name*/, ContextPtr /*context*/, bool throw_on_error) const
{
    if (throw_on_error)
        throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "There is no SHOW CREATE TABLE query for Database{}", getEngineName());
    return nullptr;
}

DiskPtr IDatabase::getDisk() const
{
    return Context::getGlobalContextInstance()->getDatabaseDisk();
}
}
