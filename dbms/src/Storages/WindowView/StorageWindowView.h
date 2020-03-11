#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeInterval.h>
#include <Storages/IStorage.h>
#include <Poco/Logger.h>
#include <ext/shared_ptr_helper.h>

#include <mutex>

namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using BlocksListPtr = std::shared_ptr<BlocksList>;
using BlocksListPtrs = std::shared_ptr<std::list<BlocksListPtr>>;

class StorageWindowView : public ext::shared_ptr_helper<StorageWindowView>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageWindowView>;
    friend class WindowViewBlockInputStream;

public:
    ~StorageWindowView() override;
    String getName() const override { return "WindowView"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const override { return database_name; }
    String getSelectDatabaseName() const { return select_database_name; }
    String getSelectTableName() const { return select_table_name; }

    ASTPtr getInnerQuery() const { return inner_query->clone(); }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    bool isTemporary() { return is_temporary; }

    /// Check we have any active readers
    /// must be called with mutex locked
    bool hasActiveUsers() { return active_ptr.use_count() > 1; }

    /// Background thread for temporary tables
    /// which drops this table if there are no users
    void startNoUsersThread(const UInt64 & timeout);
    std::mutex no_users_thread_wakeup_mutex;
    bool no_users_thread_wakeup{false};
    std::condition_variable no_users_thread_condition;

    void checkTableCanBeDropped() const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    void drop(TableStructureWriteLockHolder &) override;

    void startup() override;
    void shutdown() override;

    BlockInputStreams watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlocksListPtrs getMergeableBlocksList() { return mergeable_blocks; }
    std::shared_ptr<bool> getActivePtr() { return active_ptr; }

    /// Read new data blocks that store query result
    BlockInputStreamPtr getNewBlocksInputStreamPtr();

    BlocksPtr getNewBlocks();

    Block getHeader() const;

    StoragePtr & getParentStorage();

    static void writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context);

    ASTPtr innerQueryParser(ASTSelectQuery & inner_query);

    inline UInt32 getWindowUpperBound(UInt32 time_sec);

private:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    String window_column_name;
    String window_end_column_alias;
    Context & global_context;
    StoragePtr parent_storage;
    bool is_temporary{false};
    mutable Block sample_block;

    /// Mutex for the blocks and ready condition
    std::mutex mutex;
    std::mutex flushTableMutex;
    /// New blocks ready condition to broadcast to readers
    /// that new blocks are available
    std::condition_variable condition;

    /// Active users
    std::shared_ptr<bool> active_ptr;
    BlocksListPtrs mergeable_blocks;

    IntervalKind::Kind window_kind;
    Int64 window_num_units;
    const DateLUTImpl & time_zone;

    std::atomic<bool> has_target_table{false};
    String target_database_name;
    String target_table_name;

    static void noUsersThread(std::shared_ptr<StorageWindowView> storage, const UInt64 & timeout);
    inline void flushToTable();
    void threadFuncToTable();
    bool refreshBlockStatus();
    std::mutex no_users_thread_mutex;
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> start_no_users_thread_called{false};
    UInt64 temporary_window_view_timeout;

    Poco::Logger * log;
    Poco::Timestamp timestamp;

    BackgroundSchedulePool::TaskHolder toTableTask;
    BackgroundSchedulePool::TaskHolder toTableTask_preprocess;

    StorageWindowView(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns);
};
}
