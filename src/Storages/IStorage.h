#pragma once

#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Databases/IDatabase.h>
#include <Interpreters/CancellationCode.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableStructureLockHolder.h>
#include <Storages/CheckResults.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ColumnDependency.h>
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
struct SettingChange;
using SettingsChanges = std::vector<SettingChange>;

class AlterCommands;
class MutationCommands;
struct PartitionCommand;
using PartitionCommands = std::vector<PartitionCommand>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

class Pipe;
using Pipes = std::vector<Pipe>;

class StoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;

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
    explicit IStorage(StorageID storage_id_) : storage_id(std::move(storage_id_)) {}

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
    virtual bool supportsSampling() const { return hasSamplingKey(); }

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

    /// Returns true if there is set table TTL, any column TTL or any move TTL.
    virtual bool hasAnyTTL() const { return false; }

    /// Returns true if there is set TTL for rows.
    virtual bool hasRowsTTL() const { return false; }

    /// Optional size information of each physical column.
    /// Currently it's only used by the MergeTree family for query optimizations.
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    virtual ColumnSizeByName getColumnSizes() const { return {}; }

public: /// thread-unsafe part. lockStructure must be acquired
    virtual const ColumnsDescription & getColumns() const; /// returns combined set of columns
    virtual void setColumns(ColumnsDescription columns_); /// sets only real columns, possibly overwrites virtual ones.
    const IndicesDescription & getIndices() const;

    const ConstraintsDescription & getConstraints() const;
    void setConstraints(ConstraintsDescription constraints_);

    /// Returns storage metadata copy. Direct modification of
    /// result structure doesn't affect storage.
    virtual StorageInMemoryMetadata getInMemoryMetadata() const;

    Block getSampleBlock() const; /// ordinary + materialized.
    Block getSampleBlockWithVirtuals() const; /// ordinary + materialized + virtuals.
    Block getSampleBlockNonMaterialized() const; /// ordinary.
    Block getSampleBlockForColumns(const Names & column_names) const; /// ordinary + materialized + aliases + virtuals.

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names, bool include_virtuals = false) const;

    /// Check that all the requested names are in the table and have the correct types.
    void check(const NamesAndTypesList & columns) const;

    /// Check that all names from the intersection of `names` and `columns` are in the table and have the same types.
    void check(const NamesAndTypesList & columns, const Names & column_names) const;

    /// Check that the data block contains all the columns of the table with the correct types,
    /// contains only the columns of the table, and all the columns are different.
    /// If |need_all| is set, then checks that all the columns of the table are in the block.
    void check(const Block & block, bool need_all = false) const;

    /// Return list of virtual columns (like _part, _table, etc). In the vast
    /// majority of cases virtual columns are static constant part of Storage
    /// class and don't depend on Storage object. But sometimes we have fake
    /// storages, like Merge, which works as proxy for other storages and it's
    /// virtual columns must contain virtual columns from underlying table.
    ///
    /// User can create columns with the same name as virtual column. After that
    /// virtual column will be overriden and inaccessible.
    ///
    /// By default return empty list of columns.
    virtual NamesAndTypesList getVirtuals() const;

protected: /// still thread-unsafe part.
    void setIndices(IndicesDescription indices_);

    /// Returns whether the column is virtual - by default all columns are real.
    /// Initially reserved virtual column name may be shadowed by real column.
    bool isVirtualColumn(const String & column_name) const;


private:
    StorageID storage_id;
    mutable std::mutex id_mutex;

    ColumnsDescription columns;
    IndicesDescription indices;
    ConstraintsDescription constraints;

    StorageMetadataKeyField partition_key;
    StorageMetadataKeyField primary_key;
    StorageMetadataKeyField sorting_key;
    StorageMetadataKeyField sampling_key;

private:
    RWLockImpl::LockHolder tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const SettingSeconds & acquire_timeout) const;

public:
    /// Acquire this lock if you need the table structure to remain constant during the execution of
    /// the query. If will_add_new_data is true, this means that the query will add new data to the table
    /// (INSERT or a parts merge).
    TableStructureReadLockHolder lockStructureForShare(bool will_add_new_data, const String & query_id, const SettingSeconds & acquire_timeout);

    /// Acquire this lock at the start of ALTER to lock out other ALTERs and make sure that only you
    /// can modify the table structure. It can later be upgraded to the exclusive lock.
    TableStructureWriteLockHolder lockAlterIntention(const String & query_id, const SettingSeconds & acquire_timeout);

    /// Upgrade alter intention lock to the full exclusive structure lock. This is done by ALTER queries
    /// to ensure that no other query uses the table structure and it can be safely changed.
    void lockStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id, const SettingSeconds & acquire_timeout);

    /// Acquire the full exclusive lock immediately. No other queries can run concurrently.
    TableStructureWriteLockHolder lockExclusively(const String & query_id, const SettingSeconds & acquire_timeout);

    /** Returns stage to which query is going to be processed in read() function.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server.)
      *
      * SelectQueryInfo is required since the stage can depends on the query
      * (see Distributed() engine and optimize_skip_unused_shards).
      *
      * QueryProcessingStage::Enum required for Distributed over Distributed,
      * since it cannot return Complete for intermediate queries never.
      */
    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context) const
    {
        return getQueryProcessingStage(context, QueryProcessingStage::Complete, {});
    }
    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, const ASTPtr &) const
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
      * It is guaranteed that the structure of the table will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
      */
    virtual Pipes read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** The same as read, but returns BlockInputStreams.
     */
    BlockInputStreams readStreams(
            const Names & /*column_names*/,
            const SelectQueryInfo & /*query_info*/,
            const Context & /*context*/,
            QueryProcessingStage::Enum /*processed_stage*/,
            size_t /*max_block_size*/,
            unsigned /*num_streams*/);

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * It is guaranteed that the table structure will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
      */
    virtual BlockOutputStreamPtr write(
        const ASTPtr & /*query*/,
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
      * Must be called under lockForAlter.
      */
    virtual void truncate(const ASTPtr & /*query*/, const Context & /* context */, TableStructureWriteLockHolder &)
    {
        throw Exception("Truncate is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

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

    /** ALTER tables in the form of column changes that do not affect the change to Storage or its parameters.
      * This method must fully execute the ALTER query, taking care of the locks itself.
      * To update the table metadata on disk, this method should call InterpreterAlterQuery::updateMetadata.
      */
    virtual void alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder);

    /** Checks that alter commands can be applied to storage. For example, columns can be modified,
      * or primary key can be changes, etc.
      */
    virtual void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings);

    /** ALTER tables with regard to its partitions.
      * Should handle locks for each command on its own.
      */
    virtual void alterPartition(const ASTPtr & /* query */, const PartitionCommands & /* commands */, const Context & /* context */)
    {
        throw Exception("Partition operations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    virtual bool optimize(const ASTPtr & /*query*/, const ASTPtr & /*partition*/, bool /*final*/, bool /*deduplicate*/, const Context & /*context*/)
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

    std::atomic<bool> is_dropped{false};

    /// Does table support index for IN sections
    virtual bool supportsIndexForIn() const { return false; }

    /// Provides a hint that the storage engine may evaluate the IN-condition by using an index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */, const Context & /* query_context */) const { return false; }

    /// Checks validity of the data
    virtual CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) { throw Exception("Check query is not supported for " + getName() + " storage", ErrorCodes::NOT_IMPLEMENTED); }

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped() const {}

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkPartitionCanBeDropped(const ASTPtr & /*partition*/) {}

    /// Returns data paths if storage supports it, empty vector otherwise.
    virtual Strings getDataPaths() const { return {}; }

    /// Returns structure with partition key.
    const StorageMetadataKeyField & getPartitionKey() const;
    /// Set partition key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setPartitionKey(const StorageMetadataKeyField & partition_key_);
    /// Returns ASTExpressionList of partition key expression for storage or nullptr if there is none.
    ASTPtr getPartitionKeyAST() const { return partition_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) partition key.
    bool isPartitionKeyDefined() const;
    /// Storage has partition key.
    bool hasPartitionKey() const;
    /// Returns column names that need to be read to calculate partition key.
    Names getColumnsRequiredForPartitionKey() const;


    /// Returns structure with sorting key.
    const StorageMetadataKeyField & getSortingKey() const;
    /// Set sorting key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setSortingKey(const StorageMetadataKeyField & sorting_key_);
    /// Returns ASTExpressionList of sorting key expression for storage or nullptr if there is none.
    ASTPtr getSortingKeyAST() const { return sorting_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sorting key.
    bool isSortingKeyDefined() const;
    /// Storage has sorting key. It means, that it contains at least one column.
    bool hasSortingKey() const;
    /// Returns column names that need to be read to calculate sorting key.
    Names getColumnsRequiredForSortingKey() const;
    /// Returns columns names in sorting key specified by user in ORDER BY
    /// expression. For example: 'a', 'x * y', 'toStartOfMonth(date)', etc.
    Names getSortingKeyColumns() const;

    /// Returns structure with primary key.
    const StorageMetadataKeyField & getPrimaryKey() const;
    /// Set primary key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setPrimaryKey(const StorageMetadataKeyField & primary_key_);
    /// Returns ASTExpressionList of primary key expression for storage or nullptr if there is none.
    ASTPtr getPrimaryKeyAST() const { return primary_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sorting key.
    bool isPrimaryKeyDefined() const;
    /// Storage has primary key (maybe part of some other key). It means, that
    /// it contains at least one column.
    bool hasPrimaryKey() const;
    /// Returns column names that need to be read to calculate primary key.
    Names getColumnsRequiredForPrimaryKey() const;
    /// Returns columns names in sorting key specified by. For example: 'a', 'x
    /// * y', 'toStartOfMonth(date)', etc.
    Names getPrimaryKeyColumns() const;

    /// Returns structure with sampling key.
    const StorageMetadataKeyField & getSamplingKey() const;
    /// Set sampling key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setSamplingKey(const StorageMetadataKeyField & sampling_key_);
    /// Returns sampling expression AST for storage or nullptr if there is none.
    ASTPtr getSamplingKeyAST() const { return sampling_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sampling key.
    bool isSamplingKeyDefined() const;
    /// Storage has sampling key.
    bool hasSamplingKey() const;
    /// Returns column names that need to be read to calculate sampling key.
    Names getColumnsRequiredForSampling() const;

    /// Returns column names that need to be read for FINAL to work.
    Names getColumnsRequiredForFinal() const { return getColumnsRequiredForSortingKey(); }


    /// Returns columns, which will be needed to calculate dependencies
    /// (skip indices, TTL expressions) if we update @updated_columns set of columns.
    virtual ColumnDependencies getColumnDependencies(const NameSet & /* updated_columns */) const { return {}; }

    /// Returns storage policy if storage supports it
    virtual StoragePolicyPtr getStoragePolicy() const { return {}; }

    /// If it is possible to quickly determine exact number of rows in the table at this moment of time, then return it.
    /// Used for:
    /// - Simple count() opimization
    /// - For total_rows column in system.tables
    ///
    /// Does takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> totalRows() const
    {
        return {};
    }

    /// If it is possible to quickly determine exact number of bytes for the table on storage:
    /// - memory (approximated)
    /// - disk (compressed)
    ///
    /// Used for:
    /// - For total_bytes column in system.tables
    //
    /// Does not takes underlying Storage (if any) into account
    /// (since for Buffer we still need to know how much bytes it uses).
    virtual std::optional<UInt64> totalBytes() const
    {
        return {};
    }

private:
    /// You always need to take the next three locks in this order.

    /// If you hold this lock exclusively, you can be sure that no other structure modifying queries
    /// (e.g. ALTER, DROP) are concurrently executing. But queries that only read table structure
    /// (e.g. SELECT, INSERT) can continue to execute.
    mutable RWLock alter_intention_lock = RWLockImpl::create();

    /// It is taken for share for the entire INSERT query and the entire merge of the parts (for MergeTree).
    /// ALTER COLUMN queries acquire an exclusive lock to ensure that no new parts with the old structure
    /// are added to the table and thus the set of parts to modify doesn't change.
    mutable RWLock new_data_structure_lock = RWLockImpl::create();

    /// Lock for the table column structure (names, types, etc.) and data path.
    /// It is taken in exclusive mode by queries that modify them (e.g. RENAME, ALTER and DROP)
    /// and in share mode by other queries.
    mutable RWLock structure_lock = RWLockImpl::create();
};

}
