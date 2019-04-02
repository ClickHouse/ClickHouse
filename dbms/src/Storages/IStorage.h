#pragma once

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Databases/IDatabase.h>
#include <Storages/ITableDeclaration.h>
#include <Storages/TableStructureLockHolder.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/CancellationCode.h>
#include <shared_mutex>
#include <memory>
#include <optional>
#include <Common/ActionLock.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
}

class Context;
class IBlockInputStream;
class IBlockOutputStream;

using StorageActionBlockType = size_t;

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

class ASTCreateQuery;

class IStorage;

using StoragePtr = std::shared_ptr<IStorage>;
using StorageWeakPtr = std::weak_ptr<IStorage>;

struct Settings;

class AlterCommands;
class MutationCommands;
class PartitionCommands;


/** Storage. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, private boost::noncopyable, public ITableDeclaration
{
public:
    /// The main name of the table type (for example, StorageMergeTree).
    virtual std::string getName() const = 0;

    /// The name of the table.
    virtual std::string getTableName() const = 0;
    virtual std::string getDatabaseName() const { return {}; }  // FIXME: should be abstract method.

    /** Returns true if the storage receives data from a remote server or servers. */
    virtual bool isRemote() const { return false; }

    /** Returns true if the storage supports queries with the SAMPLE section. */
    virtual bool supportsSampling() const { return false; }

    /** Returns true if the storage supports queries with the FINAL section. */
    virtual bool supportsFinal() const { return false; }

    /** Returns true if the storage supports queries with the PREWHERE section. */
    virtual bool supportsPrewhere() const { return false; }

    /** Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas. */
    virtual bool supportsReplication() const { return false; }

    /** Returns true if the storage supports deduplication of inserted data blocks . */
    virtual bool supportsDeduplication() const { return false; }


    /// Acquire this lock if you need the table structure to remain constant during the execution of
    /// the query. If will_add_new_data is true, this means that the query will add new data to the table
    /// (INSERT or a parts merge).
    TableStructureReadLockHolder lockStructureForShare(bool will_add_new_data, const String & query_id)
    {
        TableStructureReadLockHolder result;
        if (will_add_new_data)
            result.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Read, query_id);
        result.structure_lock = structure_lock->getLock(RWLockImpl::Read, query_id);

        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
        return result;
    }

    /// Acquire this lock at the start of ALTER to lock out other ALTERs and make sure that only you
    /// can modify the table structure. It can later be upgraded to the exclusive lock.
    TableStructureWriteLockHolder lockAlterIntention(const String & query_id)
    {
        TableStructureWriteLockHolder result;
        result.alter_intention_lock = alter_intention_lock->getLock(RWLockImpl::Write, query_id);

        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
        return result;
    }

    /// Upgrade alter intention lock and make sure that no new data is inserted into the table.
    /// This is used by the ALTER MODIFY of the MergeTree storage to consistently determine
    /// the set of parts that needs to be altered.
    void lockNewDataStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id)
    {
        if (!lock_holder.alter_intention_lock)
            throw Exception("Alter intention lock for table " + getTableName() + " was not taken. This is a bug.",
                ErrorCodes::LOGICAL_ERROR);

        lock_holder.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
    }

    /// Upgrade alter intention lock to the full exclusive structure lock. This is done by ALTER queries
    /// to ensure that no other query uses the table structure and it can be safely changed.
    void lockStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id)
    {
        if (!lock_holder.alter_intention_lock)
            throw Exception("Alter intention lock for table " + getTableName() + " was not taken. This is a bug.",
                ErrorCodes::LOGICAL_ERROR);

        if (!lock_holder.new_data_structure_lock)
            lock_holder.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
        lock_holder.structure_lock = structure_lock->getLock(RWLockImpl::Write, query_id);
    }

    /// Acquire the full exclusive lock immediately. No other queries can run concurrently.
    TableStructureWriteLockHolder lockExclusively(const String & query_id)
    {
        TableStructureWriteLockHolder result;
        result.alter_intention_lock = alter_intention_lock->getLock(RWLockImpl::Write, query_id);

        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

        result.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
        result.structure_lock = structure_lock->getLock(RWLockImpl::Write, query_id);

        return result;
    }

    /** Returns stage to which query is going to be processed in read() function.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server.)
      */
    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context &) const { return QueryProcessingStage::FetchColumns; }

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
    virtual BlockInputStreams read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

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
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    virtual void drop() {}

    /** Clear the table data and leave it empty.
      * Must be called under lockForAlter.
      */
    virtual void truncate(const ASTPtr & /*query*/, const Context & /* context */)
    {
        throw Exception("Truncate is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Rename the table.
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * In this function, you need to rename the directory with the data, if any.
      * Called when the table structure is locked for write.
      */
    virtual void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/)
    {
        throw Exception("Method rename is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** ALTER tables in the form of column changes that do not affect the change to Storage or its parameters.
      * This method must fully execute the ALTER query, taking care of the locks itself.
      * To update the table metadata on disk, this method should call InterpreterAlterQuery::updateMetadata.
      */
    virtual void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context, TableStructureWriteLockHolder & table_lock_holder);

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

    bool is_dropped{false};

    /// Does table support index for IN sections
    virtual bool supportsIndexForIn() const { return false; }

    /// Provides a hint that the storage engine may evaluate the IN-condition by using an index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */, const Context & /* query_context */) const { return false; }

    /// Checks validity of the data
    virtual bool checkData() const { throw Exception("Check query is not supported for " + getName() + " storage", ErrorCodes::NOT_IMPLEMENTED); }

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped() const {}

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkPartitionCanBeDropped(const ASTPtr & /*partition*/) {}

    /** Notify engine about updated dependencies for this storage. */
    virtual void updateDependencies() {}

    /// Returns data path if storage supports it, empty string otherwise.
    virtual String getDataPath() const { return {}; }

    /// Returns ASTExpressionList of partition key expression for storage or nullptr if there is none.
    virtual ASTPtr getPartitionKeyAST() const { return nullptr; }

    /// Returns ASTExpressionList of sorting key expression for storage or nullptr if there is none.
    virtual ASTPtr getSortingKeyAST() const { return nullptr; }

    /// Returns ASTExpressionList of primary key expression for storage or nullptr if there is none.
    virtual ASTPtr getPrimaryKeyAST() const { return nullptr; }

    /// Returns sampling expression AST for storage or nullptr if there is none.
    virtual ASTPtr getSamplingKeyAST() const { return nullptr; }

    /// Returns additional columns that need to be read to calculate partition key.
    virtual Names getColumnsRequiredForPartitionKey() const { return {}; }

    /// Returns additional columns that need to be read to calculate sorting key.
    virtual Names getColumnsRequiredForSortingKey() const { return {}; }

    /// Returns additional columns that need to be read to calculate primary key.
    virtual Names getColumnsRequiredForPrimaryKey() const { return {}; }

    /// Returns additional columns that need to be read to calculate sampling key.
    virtual Names getColumnsRequiredForSampling() const { return {}; }

    /// Returns additional columns that need to be read for FINAL to work.
    virtual Names getColumnsRequiredForFinal() const { return {}; }

    using ITableDeclaration::ITableDeclaration;
    using std::enable_shared_from_this<IStorage>::shared_from_this;

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

/// table name -> table
using Tables = std::map<String, StoragePtr>;

}
