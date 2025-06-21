#pragma once

#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Databases/IDatabase.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/CheckResults.h>
#include <Storages/ColumnDependency.h>
#include <Storages/ColumnSize.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ActionLock.h>
#include <Common/RWLock.h>
#include <Common/TypePromotion.h>
#include <DataTypes/Serializations/SerializationInfo.h>

#include <optional>


namespace DB
{

using StorageActionBlockType = size_t;

class ASTCreateQuery;
class ASTInsertQuery;

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

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;

class QueryPipeline;

class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;

struct StreamLocalLimits;
class EnabledQuota;
struct SelectQueryInfo;

using NameDependencies = std::unordered_map<String, std::vector<String>>;
using DatabaseAndTableName = std::pair<String, String>;

class BackupEntriesCollector;
class RestorerFromBackup;

class ConditionSelectivityEstimator;

class ActionsDAG;

/** Storage. Describes the table. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, public TypePromotion<IStorage>, public IHints<>
{
public:
    IStorage() = delete;
    /// Storage metadata can be set separately in setInMemoryMetadata method
    explicit IStorage(StorageID storage_id_, std::unique_ptr<StorageInMemoryMetadata> metadata_ = nullptr);

    IStorage(const IStorage &) = delete;
    IStorage & operator=(const IStorage &) = delete;

    /// The main name of the table type (e.g. Memory, MergeTree, CollapsingMergeTree).
    virtual std::string getName() const = 0;

    /// The name of the table.
    StorageID getStorageID() const;

    virtual bool isMergeTree() const { return false; }

    /// Returns true if the storage receives data from a remote server or servers.
    virtual bool isRemote() const { return false; }

    /// Returns true if the storage is a view of a table or another view.
    virtual bool isView() const { return false; }

    /// Returns true if the storage is dictionary
    virtual bool isDictionary() const { return false; }

    /// Returns true if the storage supports queries with the SAMPLE section.
    virtual bool supportsSampling() const { return getInMemoryMetadataPtr()->hasSamplingKey(); }

    /// Returns true if the storage supports queries with the FINAL section.
    virtual bool supportsFinal() const { return false; }

    /// Returns true if the storage supports insert queries with the PARTITION BY section.
    virtual bool supportsPartitionBy() const { return false; }

    /// Returns true if the storage supports queries with the TTL section.
    virtual bool supportsTTL() const { return false; }

    /// Returns true if the storage supports queries with the PREWHERE section.
    virtual bool supportsPrewhere() const { return false; }

    virtual ConditionSelectivityEstimator getConditionSelectivityEstimatorByPredicate(const StorageSnapshotPtr &, const ActionsDAG *, ContextPtr) const;

    /// Returns which columns supports PREWHERE, or empty std::nullopt if all columns is supported.
    /// This is needed for engines whose aggregates data from multiple tables, like Merge.
    virtual std::optional<NameSet> supportedPrewhereColumns() const { return std::nullopt; }

    /// Returns true if the storage supports optimization of moving conditions to PREWHERE section.
    virtual bool canMoveConditionsToPrewhere() const { return supportsPrewhere(); }

    /// Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas.
    virtual bool supportsReplication() const { return false; }

    /// Returns true if the storage supports parallel insert.
    /// If false, each INSERT query will call write() only once.
    /// Different INSERT queries may write in parallel regardless of this value.
    virtual bool supportsParallelInsert() const { return false; }

    /// Returns true if the storage supports deduplication of inserted data blocks.
    virtual bool supportsDeduplication() const { return false; }

    /// Returns true if the blocks shouldn't be pushed to associated views on insert.
    virtual bool noPushingToViewsOnInserts() const { return false; }

    /// Read query returns streams which automatically distribute data between themselves.
    /// So, it's impossible for one stream run out of data when there is data in other streams.
    /// Example is StorageSystemNumbers.
    virtual bool hasEvenlyDistributedRead() const { return false; }

    /// Returns true if the storage supports reading of subcolumns of complex types.
    virtual bool supportsSubcolumns() const { return false; }
    /// Returns true if storage supports optimizations of functions by reading subcolumns.
    virtual bool supportsOptimizationToSubcolumns() const { return supportsSubcolumns(); }

    /// Returns true if the storage supports transactions for SELECT, INSERT and ALTER queries.
    /// Storage may throw an exception later if some query kind is not fully supported.
    /// This method can return true for readonly engines that return the same rows for reading (such as SystemNumbers)
    virtual bool supportsTransactions() const { return false; }

    /// Returns true if the storage supports storing of data type Object.
    virtual bool supportsDynamicSubcolumnsDeprecated() const { return false; }

    /// Returns true if the storage supports storing of dynamic subcolumns.
    virtual bool supportsDynamicSubcolumns() const { return false; }

    /// Requires squashing small blocks to large for optimal storage.
    /// This is true for most storages that store data on disk.
    virtual bool prefersLargeBlocks() const { return true; }

    /// Returns true if the storage is for system, which cannot be target of SHOW CREATE TABLE.
    virtual bool isSystemStorage() const { return false; }

    /// Returns true if asynchronous inserts are enabled for table.
    virtual bool areAsynchronousInsertsEnabled() const { return false; }

    virtual bool isSharedStorage() const { return false; }

    /// Optional size information of each physical column.
    /// Currently it's only used by the MergeTree family for query optimizations.
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    virtual ColumnSizeByName getColumnSizes() const { return {}; }

    /// Optional size information of each secondary index.
    /// Valid only for MergeTree family.
    using IndexSizeByName = std::unordered_map<std::string, IndexSize>;
    virtual IndexSizeByName getSecondaryIndexSizes() const { return {}; }

    /// Get mutable version (snapshot) of storage metadata. Metadata object is
    /// multiversion, so it can be concurrently changed, but returned copy can be
    /// used without any locks.
    virtual StorageInMemoryMetadata getInMemoryMetadata() const { return *metadata.get(); }

    /// Get immutable version (snapshot) of storage metadata. Metadata object is
    /// multiversion, so it can be concurrently changed, but returned copy can be
    /// used without any locks.
    virtual StorageMetadataPtr getInMemoryMetadataPtr() const { return metadata.get(); }

    /// Update storage metadata. Used in ALTER or initialization of Storage.
    /// Metadata object is multiversion, so this method can be called without
    /// any locks.
    void setInMemoryMetadata(const StorageInMemoryMetadata & metadata_)
    {
        metadata.set(std::make_unique<StorageInMemoryMetadata>(metadata_));
    }

    void setVirtuals(VirtualColumnsDescription virtuals_)
    {
        virtuals.set(std::make_unique<VirtualColumnsDescription>(std::move(virtuals_)));
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
    VirtualsDescriptionPtr getVirtualsPtr() const { return virtuals.get(); }
    NamesAndTypesList getVirtualsList() const { return virtuals.get()->getNamesAndTypesList(); }
    Block getVirtualsHeader() const { return virtuals.get()->getSampleBlock(); }

    Names getAllRegisteredNames() const override;

    NameDependencies getDependentViewsByColumn(ContextPtr context) const;

    /// Returns whether the column is virtual - by default all columns are real.
    /// Initially reserved virtual column name may be shadowed by real column.
    bool isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const;

    /// Modify a CREATE TABLE query to make a variant which must be written to a backup.
    virtual void applyMetadataChangesToCreateQueryForBackup(ASTPtr & create_query) const;

    /// Makes backup entries to backup the data of this storage.
    virtual void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    /// Extracts data from the backup and put it to the storage.
    virtual void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    /// Returns true if the storage supports backup/restore for specific partitions.
    virtual bool supportsBackupPartition() const { return false; }

    /// Called after all databases and tables on all replicas have been restored from backup.
    /// If this data does some background work that depends on contents of other tables, this is
    /// the place to kick off that work (and it should be paused when IStorage is created with
    /// is_restore_from_backup = true in StorageFactory::Arguments).
    virtual void finalizeRestoreFromBackup() {}

    /// Return true if there is at least one part containing lightweight deleted mask.
    virtual bool hasLightweightDeletedMask() const { return false; }

    /// Return true if storage can execute lightweight delete mutations.
    virtual bool supportsLightweightDelete() const { return false; }

    /// Return true if storage has any projection.
    virtual bool hasProjection() const { return false; }

    /// Return true if storage can execute 'DELETE FROM' mutations. This is different from lightweight delete
    /// because those are internally translated into 'ALTER UDPATE' mutations.
    virtual bool supportsDelete() const { return false; }

    /// Returns true if storage can store columns in sparse serialization.
    virtual bool supportsSparseSerialization() const { return false; }

    /// Return true if the trivial count query could be optimized without reading the data at all
    /// in totalRows() or totalRowsByPartitionPredicate() methods or with optimized reading in read() method.
    /// 'storage_snapshot' may be nullptr.
    virtual bool supportsTrivialCountOptimization(const StorageSnapshotPtr & /*storage_snapshot*/, ContextPtr /*query_context*/) const
    {
        return false;
    }

    /// Returns hints for serialization of columns accorsing to statistics accumulated by storage.
    virtual SerializationInfoByName getSerializationHints() const { return {}; }

    /// Add engine args that were inferred during storage creation to create query to avoid the same
    /// inference on server restart. For example - data format inference in File/URL/S3/etc engines.
    virtual void addInferredEngineArgsToCreateQuery(ASTs & /*args*/, const ContextPtr & /*context*/) const {}

private:
    StorageID storage_id;

    mutable std::mutex id_mutex;

    /// Multiversion storage metadata. Allows to read/write storage metadata without locks.
    MultiVersionStorageMetadataPtr metadata;

    /// Description of virtual columns. Optional, may be set in constructor.
    MultiVersionVirtualsDescriptionPtr virtuals;

protected:
    RWLockImpl::LockHolder tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const;

public:
    /// Lock table for share. This lock must be acquired if you want to be sure,
    /// that table will be not dropped while you holding this lock. It's used in
    /// variety of cases starting from SELECT queries to background merges in
    /// MergeTree.
    TableLockHolder lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout);

    /// Similar to lockForShare, but returns a nullptr if the table is dropped while
    /// acquiring the lock instead of raising a TABLE_IS_DROPPED exception
    TableLockHolder tryLockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout);

    /// Lock table for alter. This lock must be acquired in ALTER queries to be
    /// sure, that we execute only one simultaneous alter. Doesn't affect share lock.
    using AlterLockHolder = std::unique_lock<std::timed_mutex>;
    AlterLockHolder lockForAlter(const std::chrono::milliseconds & acquire_timeout);
    std::optional<AlterLockHolder> tryLockForAlter(const std::chrono::milliseconds & acquire_timeout);

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
      *  for example, the request can be partially processed on a remote server, or an aggregate projection.)
      *
      * SelectQueryInfo is required since the stage can depends on the query
      * (see Distributed() engine and optimize_skip_unused_shards,
      *  see also MergeTree engine and projection optimization).
      * And to store optimized cluster (after optimize_skip_unused_shards).
      * It will also store needed stuff for projection query pipeline.
      *
      * QueryProcessingStage::Enum required for Distributed over Distributed,
      * since it cannot return Complete for intermediate queries ever.
      */
    virtual QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
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
    virtual Pipe watch(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum & /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /// Returns true if FINAL modifier must be added to SELECT query depending on required columns.
    /// It's needed for ReplacingMergeTree wrappers such as MaterializedPostrgeSQL
    virtual bool needRewriteQueryWithFinal(const Names & /*column_names*/) const { return false; }

private:
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
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /// Should we process blocks of data returned by the storage in parallel
    /// even when the storage returned only one stream of data for reading?
    /// It is beneficial, for example, when you read from a file quickly,
    /// but then do heavy computations on returned blocks.
    ///
    /// This is enabled by default, but in some cases shouldn't be done (for
    /// example it is disabled for all system tables, since it is pretty
    /// useless).
    virtual bool parallelizeOutputAfterReading(ContextPtr) const { return !isSystemStorage(); }

public:
    /// Other version of read which adds reading step to query plan.
    /// Default implementation creates ReadFromStorageStep and uses usual read.
    /// Can be called after `shutdown`, but not after `drop`.
    virtual void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * metadata_snapshot is consistent snapshot of table metadata, it should be
      * passed in all parts of the returned streams. Storage metadata can be
      * changed during lifetime of the returned streams, but the snapshot is
      * guaranteed to be immutable.
      *
      * async_insert - set to true if the write is part of async insert flushing
      */
    virtual SinkToStoragePtr write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*context*/,
        bool /*async_insert*/);

    /** Writes the data to a table in distributed manner.
      * It is supposed that implementation looks into SELECT part of the query and executes distributed
      * INSERT SELECT if it is possible with current storage as a receiver and query SELECT part as a producer.
      *
      * Returns query pipeline if distributed writing is possible, and nullptr otherwise.
      */
    virtual std::optional<QueryPipeline> distributedWrite(
        const ASTInsertQuery & /*query*/,
        ContextPtr /*context*/);

    /** Delete the table data. Called before deleting the directory with the data.
      * The method can be called only after detaching table from Context (when no queries are performed with table).
      * The table is not usable during and after call to this method.
      * If some queries may still use the table, then it must be called under exclusive lock.
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    virtual void drop() {}

    virtual void dropInnerTableIfAny(bool /* sync */, ContextPtr /* context */) {}

    /** Clear the table data and leave it empty.
      * Must be called under exclusive lock (lockExclusively).
      */
    virtual void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* context */,
        TableExclusiveLockHolder &);

    virtual void checkTableCanBeRenamed(const StorageID & /*new_name*/) const {}

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
    virtual void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder);

    /// Updates metadata that can be changed by other processes
    /// Return true if external metadata exists and was updated.
    virtual bool updateExternalDynamicMetadataIfExists(ContextPtr /* context */) { return false; }

    /** Checks that alter commands can be applied to storage. For example, columns can be modified,
      * or primary key can be changes, etc.
      */
    virtual void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const;

    /**
      * Checks that mutation commands can be applied to storage.
      */
    virtual void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const;

    /** ALTER tables with regard to its partitions.
      * Should handle locks for each command on its own.
      */
    virtual Pipe alterPartition(
        const StorageMetadataPtr & /* metadata_snapshot */,
        const PartitionCommands & /* commands */,
        ContextPtr /* context */);

    /// Checks that partition commands can be applied to storage.
    virtual void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr context) const;

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    virtual bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        bool /*cleanup*/,
        ContextPtr /*context*/);

    /// Mutate the table contents
    virtual void mutate(const MutationCommands &, ContextPtr);

    /// Cancel a mutation.
    virtual CancellationCode killMutation(const String & /*mutation_id*/);

    virtual void waitForMutation(const String & /*mutation_id*/, bool /*wait_for_another_mutation*/);

    virtual void setMutationCSN(const String & /*mutation_id*/, UInt64 /*csn*/);

    /// Cancel a part move to shard.
    virtual CancellationCode killPartMoveToShard(const UUID & /*task_uuid*/);

    /** If the table have to do some complicated work on startup,
      *  that must be postponed after creation of table object
      *  (like launching some background threads),
      *  do it in this method.
      * You should call this method after creation of object.
      * By default, does nothing.
      * Cannot be called simultaneously by multiple threads.
      */
    virtual void startup() {}

    /**
      * If the storage requires some complicated work on destroying,
      * then you have two virtual methods:
      * - flushAndPrepareForShutdown()
      * - shutdown()
      *
      * @see shutdown()
      * @see flushAndPrepareForShutdown()
      */
    void flushAndShutdown(bool is_drop = false)
    {
        flushAndPrepareForShutdown();
        shutdown(is_drop);
    }

    /** If the table have to do some complicated work when destroying an object - do it in advance.
      * For example, if the table contains any threads for background work - ask them to complete and wait for completion.
      * By default, does nothing.
      * Can be called simultaneously from different threads, even after a call to drop().
      */
    virtual void shutdown(bool is_drop = false) { UNUSED(is_drop); } // NOLINT

    /// Called before shutdown() to flush data to underlying storage.
    /// Might be called multiple times; only the first call needs to be processed.
    /// Data in memory need to be persistent. Any background work that affects other tables
    /// (e.g. materialized view refreshes that create/drop tables) needs to be stopped.
    virtual void flushAndPrepareForShutdown() {}

    /// Asks table to stop executing some action identified by action_type
    /// If table does not support such type of lock, and empty lock is returned
    virtual ActionLock getActionLock(StorageActionBlockType /* action_type */)
    {
        return {};
    }

    /// Call when lock from previous method removed
    virtual void onActionLockRemove(StorageActionBlockType /* action_type */) {}

    std::atomic<bool> is_dropped{false};
    std::atomic<bool> is_detached{false};
    std::atomic<bool> is_being_restarted{false};

    /** A list of tasks to check a validity of data.
      * Each IStorage implementation may interpret this task in its own way.
      * E.g. for some storages it's a list of files in filesystem, for others it can be a list of parts.
      * Also it may hold resources (e.g. locks) required during check.
      */
    struct DataValidationTasksBase
    {
        /// Number of entries left to check.
        /// It decreases after each call to checkDataNext().
        virtual size_t size() const = 0;
        virtual ~DataValidationTasksBase() = default;
    };

    using DataValidationTasksPtr = std::shared_ptr<DataValidationTasksBase>;

    /// Specifies to check all data / partition / part
    using CheckTaskFilter = std::variant<std::monostate, ASTPtr, String>;
    virtual DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & /* check_task_filter */, ContextPtr /* context */);

    /** Executes one task from the list.
      * If no tasks left - returns nullopt.
      * Note: Function `checkDataNext` is accessing `check_task_list` thread-safely,
      *   and can be called simultaneously for the same `getCheckTaskList` result
      *   to process different tasks in parallel.
      * Usage:
      *
      * auto check_task_list = storage.getCheckTaskList({}, context);
      * size_t total_tasks = check_task_list->size();
      * while (true)
      * {
      *     size_t tasks_left = check_task_list->size();
      *     std::cout << "Checking data: " << (total_tasks - tasks_left) << " / " << total_tasks << " tasks done." << std::endl;
      *     auto result = storage.checkDataNext(check_task_list);
      *     if (!result)
      *         break;
      *     doSomething(*result);
      * }
      */
    virtual std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list);

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const {}
    /// Similar to above but checks for DETACH. It's only used for DICTIONARIES.
    virtual void checkTableCanBeDetached() const {}

    /// Returns true if Storage may store some data on disk.
    /// NOTE: may not be equivalent to !getDataPaths().empty()
    virtual bool storesDataOnDisk() const { return false; }

    /// Returns data paths if storage supports it, empty vector otherwise.
    virtual Strings getDataPaths() const { return {}; }

    /// Returns storage policy if storage supports it.
    virtual StoragePolicyPtr getStoragePolicy() const { return {}; }

    /// Returns true if all disks of storage are read-only or write-once.
    /// NOTE: write-once also does not support INSERTs/merges/... for MergeTree
    virtual bool isStaticStorage() const;

    /// If it is possible to quickly determine exact number of rows in the table at this moment of time, then return it.
    /// Used for:
    /// - Simple count() optimization
    /// - For total_rows column in system.tables
    ///
    /// Does takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> totalRows(ContextPtr) const { return {}; }

    /// Same as above but also take partition predicate into account.
    virtual std::optional<UInt64> totalRowsByPartitionPredicate(const ActionsDAG &, ContextPtr) const { return {}; }

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
    virtual std::optional<UInt64> totalBytes(ContextPtr) const { return {}; }

    /// If it is possible to quickly determine exact number of uncompressed bytes for the table on storage:
    /// - disk (uncompressed)
    ///
    /// Used for:
    /// - For total_bytes_uncompressed column in system.tables
    ///
    /// Does not take underlying Storage (if any) into account
    virtual std::optional<UInt64> totalBytesUncompressed(const Settings &) const { return {}; }

    /// Number of rows INSERTed since server start.
    ///
    /// Does not take the underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeRows() const { return {}; }

    /// Number of bytes INSERTed since server start.
    ///
    /// Does not take the underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeBytes() const { return {}; }

    /// Creates a storage snapshot from given metadata.
    virtual StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const
    {
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);
    }

    /// Creates a storage snapshot from given metadata and columns, which are used in query.
    virtual StorageSnapshotPtr getStorageSnapshotForQuery(const StorageMetadataPtr & metadata_snapshot, const ASTPtr & /*query*/, ContextPtr query_context) const
    {
        return getStorageSnapshot(metadata_snapshot, query_context);
    }

    /// Creates a storage snapshot but without holding a data specific to storage.
    virtual StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
    {
        return getStorageSnapshot(metadata_snapshot, query_context);
    }

    /// Re initialize disks in case the underlying storage policy changed
    virtual bool initializeDiskOnConfigChange(const std::set<String> & /*new_added_disks*/) { return true; }

    /// A helper to implement read()
    static void readFromPipe(
        QueryPlan & query_plan,
        Pipe pipe,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        std::shared_ptr<IStorage> storage_);

private:
    /// Lock required for alter queries (lockForAlter).
    /// Allows to execute only one simultaneous alter query.
    mutable std::timed_mutex alter_lock;

    /// Lock required for drop queries. Every thread that want to ensure, that
    /// table is not dropped have to table this lock for read (lockForShare).
    /// DROP-like queries take this lock for write (lockExclusively), to be sure
    /// that all table threads finished.
    mutable RWLock drop_lock = RWLockImpl::create();
};

}
