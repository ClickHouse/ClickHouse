#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

#include <mutex>
#include <vector>


namespace DB
{

/** Writable system table `system.template_queries`: a catalog of query templates.
  *
  * Built-in rows are compiled into the binary (see TemplateQueriesBuiltin.h) and are read-only.
  * User rows are added with INSERT and removed with `DELETE FROM system.template_queries WHERE name = '...'`
  * (equality on `name` only). They are stored in a TSV file `<server path>/template_queries.tsv`,
  * which is rewritten atomically (temporary file + rename) on every change and loaded lazily on first use.
  *
  * Inserting a name that already exists (built-in or user) throws; there is no replace semantics.
  * Deleting a built-in name throws; deleting a non-existent name is a no-op.
  */
class StorageSystemTemplateQueries final : public IStorageSystemOneBlock
{
public:
    StorageSystemTemplateQueries(const StorageID & table_id_, ColumnsDescription columns_description_, String file_path_);

    String getName() const override { return "SystemTemplateQueries"; }

    static ColumnsDescription getColumnsDescription();

    bool isSystemStorage() const override { return true; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder, DDLGuardPtr & ddl_guard) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;
    bool supportsDelete() const override { return true; }

    /// A backup keeps the user rows. A restore adds the rows whose names are not taken yet.
    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;

private:
    friend class TemplateQueriesSink;

    struct UserQuery
    {
        String name;
        String category;
        String description;
        String query;
        UInt32 created_at = 0;
    };

    /// Must be called with `mutex` held.
    void loadIfNeeded() const;
    /// Replaces the file and then the cached rows. Must be called with `mutex` held.
    void saveToFile(std::vector<UserQuery> rows) const;

    static void writeRows(const std::vector<UserQuery> & rows, WriteBuffer & out);
    static std::vector<UserQuery> readRows(ReadBuffer & in);

    void addUserQueries(std::vector<UserQuery> new_rows);
    void restoreUserQueries(std::vector<UserQuery> backup_rows);
    void deleteUserQuery(const String & name);

    const String file_path;

    mutable std::mutex mutex;
    mutable bool loaded = false;
    mutable std::vector<UserQuery> user_queries;
};

}
