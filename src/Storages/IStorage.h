#pragma once

#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Databases/IDatabase.h>
#include <Interpreters/CancellationCode.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/TableLockHolder.h>
#include <Storages/CheckResults.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ColumnDependency.h>
#include <Storages/SelectQueryDescription.h>
#include <Common/ActionLock.h>
#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Common/TypePromotion.h>

#include <optional>
#include <shared_mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class Context;

using StorageActionBlockType = size_t;

class ASTCreateQuery;

struct Settings;

class AlterCommands;
class MutationCommands;
struct PartitionCommand;
using PartitionCommands = std::vector<PartitionCommand>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

class Pipe;
class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class StoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

struct StreamLocalLimits;
class EnabledQuota;
struct SelectQueryInfo;

struct ColumnSize
{
    size_t marks = 0;
    size_t data_compressed = 0;
    size_t data_uncompressed = 0;

    void add(const ColumnSize & other)
    {
        marks += other.marks;
        data_compressed += other.data_compressed;
        data_uncompressed += other.data_uncompressed;
    }
};

/** Storage. Describes the table. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, public TypePromotion<IStorage>
{
public:
    IStorage() = delete;
    /// Storage metadata can be set separately in setInMemoryMetadata method
    explicit IStorage(StorageID storage_id_)
        : storage_id(std::move(storage_id_))
        , metadata(std::make_unique<StorageInMemoryMetadata>()) {} //-V730

    virtual ~IStorage() = default;
    IStorage(const IStorage &) = delete;
    IStorage & operator=(const IStorage &) = delete;

    /// The main name of the table type (for example, StorageMergeTree).
    virtual std::string getName() const = 0;

    /// The name of the table.
    StorageID getStorageID() const;

    /// Returns true if the storage receives data from a remote server or servers.
    virtual bool isRemote() const { return false; }

    /// Returns true if the storage is a view of a table or another view.
    virtual bool isView() const { return false; }

    /// Returns true if the storage supports queries with the SAMPLE section.
    virtual bool supportsSampling() const { return getInMemoryMetadataPtr()->hasSamplingKey(); }

    /// Returns true if the storage supports queries with the FINAL section.
    virtual bool supportsFinal() const { return false; }

    /// Returns true if the storage supports queries with the PREWHERE section.
    virtual bool supportsPrewhere() const { return false; }

    /// Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas.
    virtual bool supportsReplication() const { return false; }

    /// Returns true if the storage supports parallel insert.
    virtual bool supportsParallelInsert() const { return false; }

    /// Returns true if the storage supports deduplication of inserted data blocks.
    virtual bool supportsDeduplication() const { return false; }

    /// Returns true if the storage supports settings.
    virtual bool supportsSettings() const { return false; }

    /// Returns true if the blocks shouldn't be pushed to associated views on insert.
    virtual bool noPushingToViews() const { return false; }

    /// Read query returns streams which automatically distribute data between themselves.
    /// So, it's impossible for one stream run out of data when there is data in other streams.
    /// Example is StorageSystemNumbers.
    virtual bool hasEvenlyDistributedRead() const { return false; }


    /// Optional size information of each physical column.
    /// Currently it's only used by the MergeTree family for query optimizations.
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    virtual ColumnSizeByName getColumnSizes() const { return {}; }

    /// Get mutable version (snapshot) of storage metadata. Metadata object is
    /// multiversion, so it can be concurrently changed, but returned copy can be
    /// used without any locks.
    StorageInMemoryMetadata getInMemoryMetadata() const { return *metadata.get(); }

    /// Get immutable version (snapshot) of storage metadata. Metadata object is
    /// multiversion, so it can be concurrently changed, but returned copy can be
    /// used without any locks.
    StorageMetadataPtr getInMemoryMetadataPtr() const { return metadata.get(); }

    /// Update storage metadata. Used in ALTER or initialization of Storage.
    /// Metadata object is multiversion, so this method can be called without
    /// any locks.
    void setInMemoryMetadata(const StorageInMemoryMetadata & metadata_)
    {
        metadata.set(std::make_unique<StorageInMemoryMetadata>(metadata_));
    }


    /// Return list of virtual columns (like _part, _table, etc). In the vast
    /// majority of cases virtual columns are static constant part of Storage
    /// class and don't depend on Storage object. But sometimes we have fake
    /// storages, like Merge, which works as proxy for other storages and it's
    /// virtual columns must contain virtual columns from underlying table.
    ///
    /// User can create columns with the same name as virtual column. After that
    /// virtual column will be overridden and inaccessible.
    ///
    /// By default return empty list of columns.
    virtual NamesAndTypesList getVirtuals() const;

protected:

    /// Returns whether the column is virtual - by default all columns are real.
    /// Initially reserved virtual column name may be shadowed by real column.
    bool isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const;


private:
    StorageID storage_id;
    mutable std::mutex id_mutex;

    /// Multiversion storage metadata. Allows to read/write storage metadata
    /// without locks.
    MultiVersionStorageMetadataPtr metadata;

    RWLockImpl::LockHolder tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const;

public:
    /// Lock table for share. This lock must be acuqired if you want to be sure,
    /// that table will be not dropped while you holding this lock. It's used in
    /// variety of cases starting from SELECT queries to background merges in
    /// MergeTree.
    TableLockHolder lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout);

    /// Lock table for alter. This lock must be acuqired in ALTER queries to be
    /// sure, that we execute only one simultaneous alter. Doesn't affect share lock.
    TableLockHolder lockForAlter(const String & query_id, const std::chrono::milliseconds & acquire_timeout);

    /// Lock table exclusively. This lock must be acquired if you want to be
    /// sure, that no other thread (SELECT, merge, ALTER, etc.) doing something
    /// with table. For example it allows to wait all threads before DROP or
    /// truncate query.
    ///
    /// NOTE: You have to be 100% sure that you need this lock. It's extremely
    /// heavyweight and makes table irresponsive.
    TableExclusiveLockHolder lockExclusively(const String & query_id, const std::chrono::milliseconds & acquire_timeout);

    /** Returns stage to which query is going to be processed in read() function.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server.)
      *
      * SelectQueryInfo is required since the stage can depends on the query
      * (see Distributed() engine and optimize_skip_unused_shards).
      * And to store optimized cluster (after optimize_skip_unused_shards).
      *
      * QueryProcessingStage::Enum required for Distributed over Distributed,
      * since it cannot return Complete for intermediate queries never.
      */
    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, SelectQueryInfo &) const
    {
        return QueryProcessingStage::FetchColumns;
    }

    /** Watch live changes to the table.
     * Accepts a list of columns to read, as well as a description of the query,
     *  from which information can be extracted about how to retrieve data
     *  (indexes, locks, etc.)
     * Returns a stream with which you can read data sequentially
     *  or multiple streams for parallel data reading.
     * The `processed_stage` info is also written to what stage the request was processed.
     * (Normally, the function only reads the columns from the list, but in other cases,
     *  for example, the request can be partially processed on a remote server.)
     *
     * context contains settings for one query.
     * Usually Storage does not care about these settings, since they are used in the interpreter.
     * But, for example, for distributed query processing, the settings are passed to the remote server.
     *
     * num_streams - a recommendation, how many streams to return,
     *  if the storage can return a different number of streams.
     *
     * It is guaranteed that the structure of the table will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
     */
    virtual BlockInputStreams watch(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum & /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception("Method watch is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Read a set of columns from the table.
      * Accepts a list of columns to read, as well as a description of the query,
      *  from which information can be extracted about how to retrieve data
      *  (indexes, locks, etc.)
      * Returns a stream with which you can read data sequentially
      *  or multiple streams for parallel data reading.
      * The `processed_stage` must be the result of getQueryProcessingStage() function.
      *
      * context contains settings for one query.
      * Usually Storage does not care about these settings, since they are used in the interpreter.
      * But, for example, for distributed query processing, the settings are passed to the remote server.
      *
      * num_streams - a recommendation, how many streams to return,
      *  if the storage can return a different number of streams.
      *
      * metadata_snapshot is consistent snapshot of table metadata, it should be
      * passed in all parts of the returned pipeline. Storage metadata can be
      * changed during lifetime of the returned pipeline, but the snapshot is
      * guaranteed to be immutable.
      */
    virtual Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/);

    /// Other version of read which adds reading step to query plan.
    /// Default implementation creates ReadFromStorageStep and uses usual read.
    virtual void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/);

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * metadata_snapshot is consistent snapshot of table metadata, it should be
      * passed in all parts of the returned streams. Storage metadata can be
      * changed during lifetime of the returned streams, but the snapshot is
      * guaranteed to be immutable.
      */
    virtual BlockOutputStreamPtr write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const Context & /*context*/)
    {
        throw Exception("Method write is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Delete the table data. Called before deleting the directory with the data.
      * The method can be called only after detaching table from Context (when no queries are performed with table).
      * The table is not usable during and after call to this method.
      * If some queries may still use the table, then it must be called under exclusive lock.
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    virtual void drop() {}

    /** Clear the table data and leave it empty.
      * Must be called under exclusive lock (lockExclusively).
      */
    virtual void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        const Context & /* context */,
        TableExclusiveLockHolder &)
    {
        throw Exception("Truncate is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void checkTableCanBeRenamed() const {}

    /** Rename the table.
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * In this function, you need to rename the directory with the data, if any.
      * Called when the table structure is locked for write.
      * Table UUID must remain unchanged, unless table moved between Ordinary and Atomic databases.
      */
    virtual void rename(const String & /*new_path_to_table_data*/, const StorageID & new_table_id)
    {
        renameInMemory(new_table_id);
    }

    /**
     * Just updates names of database and table without moving any data on disk
     * Can be called directly only from DatabaseAtomic.
     */
    virtual void renameInMemory(const StorageID & new_table_id);

    /** ALTER tables in the form of column changes that do not affect the change
      * to Storage or its parameters. Executes under alter lock (lockForAlter).
      */
    virtual void alter(const AlterCommands & params, const Context & context, TableLockHolder & alter_lock_holder);

    /** Checks that alter commands can be applied to storage. For example, columns can be modified,
      * or primary key can be changes, etc.
      */
    virtual void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) const;

    /** ALTER tables with regard to its partitions.
      * Should handle locks for each command on its own.
      */
    virtual Pipe alterPartition(
        const StorageMetadataPtr & /* metadata_snapshot */,
        const PartitionCommands & /* commands */,
        const Context & /* context */);

    /// Checks that partition commands can be applied to storage.
    virtual void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const;

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    virtual bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Context & /*context*/)
    {
        throw Exception("Method optimize is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Mutate the table contents
    virtual void mutate(const MutationCommands &, const Context &)
    {
        throw Exception("Mutations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Cancel a mutation.
    virtual CancellationCode killMutation(const String & /*mutation_id*/)
    {
        throw Exception("Mutations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** If the table have to do some complicated work on startup,
      *  that must be postponed after creation of table object
      *  (like launching some background threads),
      *  do it in this method.
      * You should call this method after creation of object.
      * By default, does nothing.
      * Cannot be called simultaneously by multiple threads.
      */
    virtual void startup() {}

    /** If the table have to do some complicated work when destroying an object - do it in advance.
      * For example, if the table contains any threads for background work - ask them to complete and wait for completion.
      * By default, does nothing.
      * Can be called simultaneously from different threads, even after a call to drop().
      */
    virtual void shutdown() {}

    /// Asks table to stop executing some action identified by action_type
    /// If table does not support such type of lock, and empty lock is returned
    virtual ActionLock getActionLock(StorageActionBlockType /* action_type */)
    {
        return {};
    }

    /// Call when lock from previous method removed
    virtual void onActionLockRemove(StorageActionBlockType /* action_type */) {}

    std::atomic<bool> is_dropped{false};

    /// Does table support index for IN sections
    virtual bool supportsIndexForIn() const { return false; }

    /// Provides a hint that the storage engine may evaluate the IN-condition by using an index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */, const Context & /* query_context */, const StorageMetadataPtr & /* metadata_snapshot */) const { return false; }

    /// Checks validity of the data
    virtual CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) { throw Exception("Check query is not supported for " + getName() + " storage", ErrorCodes::NOT_IMPLEMENTED); }

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped() const {}
    /// Similar to above but checks for DETACH. It's only used for DICTIONARIES.
    virtual void checkTableCanBeDetached() const {}

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkPartitionCanBeDropped(const ASTPtr & /*partition*/) {}

    /// Returns true if Storage may store some data on disk.
    /// NOTE: may not be equivalent to !getDataPaths().empty()
    virtual bool storesDataOnDisk() const { return false; }

    /// Returns data paths if storage supports it, empty vector otherwise.
    virtual Strings getDataPaths() const { return {}; }

    /// Returns storage policy if storage supports it.
    virtual StoragePolicyPtr getStoragePolicy() const { return {}; }

    /// If it is possible to quickly determine exact number of rows in the table at this moment of time, then return it.
    /// Used for:
    /// - Simple count() opimization
    /// - For total_rows column in system.tables
    ///
    /// Does takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> totalRows(const Settings &) const { return {}; }

    /// Same as above but also take partition predicate into account.
    virtual std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, const Context &) const { return {}; }

    /// If it is possible to quickly determine exact number of bytes for the table on storage:
    /// - memory (approximated, resident)
    /// - disk (compressed)
    ///
    /// Used for:
    /// - For total_bytes column in system.tables
    //
    /// Does not takes underlying Storage (if any) into account
    /// (since for Buffer we still need to know how much bytes it uses).
    ///
    /// Memory part should be estimated as a resident memory size.
    /// In particular, alloctedBytes() is preferable over bytes()
    /// when considering in-memory blocks.
    virtual std::optional<UInt64> totalBytes(const Settings &) const { return {}; }

    /// Number of rows INSERTed since server start.
    ///
    /// Does not takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeRows() const { return {}; }

    /// Number of bytes INSERTed since server start.
    ///
    /// Does not takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeBytes() const { return {}; }

private:
    /// Lock required for alter queries (lockForAlter). Always taken for write
    /// (actually can be replaced with std::mutex, but for consistency we use
    /// RWLock). Allows to execute only one simultaneous alter query. Also it
    /// should be taken by DROP-like queries, to be sure, that all alters are
    /// finished.
    mutable RWLock alter_lock = RWLockImpl::create();

    /// Lock required for drop queries. Every thread that want to ensure, that
    /// table is not dropped have to table this lock for read (lockForShare).
    /// DROP-like queries take this lock for write (lockExclusively), to be sure
    /// that all table threads finished.
    mutable RWLock drop_lock = RWLockImpl::create();
};

}
