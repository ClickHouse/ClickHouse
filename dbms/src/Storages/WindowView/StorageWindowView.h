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
class WindowViewBlockInputStream;
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

    ASTPtr getInnerQuery() const { return inner_query->clone(); }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    bool isTemporary() { return is_temporary; }

    bool hasActiveUsers() { return active_ptr.use_count() > 1; }

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
    BlockInputStreamPtr getNewBlocksInputStreamPtr(UInt32 timestamp_);

    BlockInputStreamPtr getNewBlocksInputStreamPtrInnerTable(UInt32 timestamp_);

    BlocksPtr getNewBlocks();

    Block getHeader() const;

    StoragePtr& getParentStorage()
    {
        if (parent_storage == nullptr)
            parent_storage = global_context.getTable(select_table_id);
        return parent_storage;
    }

    StoragePtr& getInnerStorage()
    {
        if (inner_storage == nullptr && !inner_table_id.empty())
            inner_storage = global_context.getTable(inner_table_id);
        return inner_storage;
    }

    static void writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context);

    static void writeIntoWindowViewInnerTable(StorageWindowView & window_view, const Block & block, const Context & context);

    ASTPtr innerQueryParser(ASTSelectQuery & inner_query);

    std::shared_ptr<ASTCreateQuery> generateInnerTableCreateQuery(const ASTCreateQuery & inner_create_query, const String & database_name, const String & table_name);

    inline UInt32 getWindowLowerBound(UInt32 time_sec, int window_id_skew = 0);
    inline UInt32 getWindowUpperBound(UInt32 time_sec, int window_id_skew = 0);
    inline UInt32 getWatermark(UInt32 time_sec);

private:
    StorageID select_table_id = StorageID::createEmpty();
    ASTPtr inner_query;
    ASTPtr fetch_column_query;
    String window_column_name;
    String timestamp_column_name;
    Context & global_context;
    StoragePtr parent_storage;
    StoragePtr inner_storage;
    bool is_temporary{false};
    mutable Block sample_block;

    /// Mutex for the blocks and ready condition
    std::mutex mutex;
    std::mutex flush_table_mutex;
    std::mutex fire_signal_mutex;
    std::list<UInt32> fire_signal;
    std::list<WindowViewBlockInputStream *> watch_streams;
    /// New blocks ready condition to broadcast to readers
    /// that new blocks are available
    std::condition_variable condition;

    /// Active users
    std::shared_ptr<bool> active_ptr;
    BlocksListPtrs mergeable_blocks;

    IntervalKind::Kind window_kind;
    Int64 window_num_units;
    IntervalKind::Kind watermark_kind;
    Int64 watermark_num_units = 0;
    const DateLUTImpl & time_zone;

    StorageID target_table_id = StorageID::createEmpty();
    StorageID inner_table_id = StorageID::createEmpty();

    void flushToTable(UInt32 timestamp_);
    void clearInnerTable();
    void threadFuncToTable();
    void threadFuncClearInnerTable();
    void threadFuncFire();
    void addFireSignal(UInt32 timestamp_);

    std::atomic<bool> shutdown_called{false};
    UInt64 inner_table_clear_interval;

    Poco::Timestamp timestamp;

    BackgroundSchedulePool::TaskHolder toTableTask;
    BackgroundSchedulePool::TaskHolder innerTableClearTask;
    BackgroundSchedulePool::TaskHolder fireTask;

    StorageWindowView(
        const StorageID & table_id_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns,
        bool attach_);
};
}
