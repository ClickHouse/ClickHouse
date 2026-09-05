#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

#include <functional>
#include <mutex>


namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class Set;
using SetPtr = std::shared_ptr<Set>;


/** Common part of StorageSet and StorageJoin.
  */
class StorageSetOrJoinBase : public StorageWithCommonVirtualColumns
{
    friend class SetOrJoinSink;

public:
    static VirtualColumnsDescription createVirtuals();

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {path}; }
    Disks getDataDisks() const override { return {disk}; }

    /// Throw if the query behind `context` cannot update the live state, for example because it is
    /// reading from this table. Checked before the staged backup of an `INSERT` is published.
    virtual void checkInsertIsPossible(ContextPtr /*context*/) const {}

protected:
    StorageSetOrJoinBase(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool persistent_);

    DiskPtr disk;
    String path;
    bool persistent;

    std::atomic<UInt64> increment = 0;    /// For the backup file names.

    /// Serializes the operations that publish or roll back committed backups: an `INSERT`
    /// publishing its staged backup, the rollback of a failed `INSERT`, and (for `Join`)
    /// mutations and truncation. Without it, the rollback of one failed insert could swap
    /// the live state while another insert is still replaying its own committed backup,
    /// so that insert would apply the same rows twice.
    mutable std::mutex mutate_mutex;

    /// Restore from backup.
    void restore();

    /// Read every committed backup in insertion order, optionally skipping one file by name. The
    /// caller owns the destination state, so it can keep an update private until it has been
    /// built successfully.
    void forEachBackupBlock(const std::function<void(const Block &)> & callback, const String & exclude_file_name = {}) const;

    /// Read the blocks of a single backup file in insertion order.
    void forEachBlockInBackupFile(const String & file_path, const std::function<void(const Block &)> & callback) const;

    /// Apply the just-promoted backup file of an `INSERT` to the live state, with the strong
    /// exception guarantee: on failure the live state has been restored to what it was before and
    /// the backup file has been removed, so a failed `INSERT` never publishes any rows. Called
    /// under `mutate_mutex`.
    virtual void publishBackup(const String & backup_file_path, ContextPtr context) = 0;

    void restoreFromFile(const String & file_path, ContextPtr context = nullptr);

private:
    /// Insert the block into the state.
    virtual void insertBlock(const Block & block, ContextPtr context) = 0;
    /// Call after all blocks were inserted.
    virtual void finishInsert() = 0;
    virtual size_t getSize(ContextPtr context) const = 0;
};


/** Lets you save the set for later use on the right side of the IN statement.
  * When inserted into a table, the data will be inserted into the set,
  *  and also written to a file-backup, for recovery after a restart.
  * Reading from the table is not possible directly - it is possible to specify only the right part of the IN statement.
  */
class StorageSet final : public StorageSetOrJoinBase
{
public:
    StorageSet(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool persistent_);

    String getName() const override { return "Set"; }

    /// Access the insides.
    SetPtr getSet() const;

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

private:
    /// Allows to concurrently truncate the set and work (read/fill) the existing set.
    mutable std::mutex mutex;
    SetPtr set TSA_GUARDED_BY(mutex);

    void insertBlock(const Block & block, ContextPtr) override;
    void finishInsert() override;
    size_t getSize(ContextPtr) const override;
    void publishBackup(const String & backup_file_path, ContextPtr context) override;

    /// Build a fresh state from the committed backups and swap it in.
    void rebuildFromBackups();
};

}
