#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeInterval.h>
#include <Interpreters/InterpreterSelectQuery.h>
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
    friend class TimestampTransformation;
    friend class WatermarkBlockInputStream;
    friend class WindowViewBlockInputStream;

public:
    ~StorageWindowView() override;
    String getName() const override { return "WindowView"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    void checkTableCanBeDropped() const override;

    void drop(TableStructureWriteLockHolder &) override;

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

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

    BlockInputStreamPtr getNewBlocksInputStreamPtr(UInt32 watermark);

    static void writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context);

private:
    ASTPtr inner_query;
    ASTPtr final_query;
    ASTPtr fetch_column_query;

    Context & global_context;
    bool is_proctime{true};
    bool is_time_column_now;
    bool is_tumble; // false if is hop
    std::atomic<bool> shutdown_called{false};
    mutable Block sample_block;
    mutable Block mergeable_sample_block;
    UInt64 clean_interval;
    const DateLUTImpl & time_zone;
    UInt32 max_timestamp = 0;
    UInt32 max_watermark = 0;
    bool is_watermark_strictly_ascending{false};
    bool is_watermark_ascending{false};
    bool is_watermark_bounded{false};
    UInt32 next_fire_signal;
    std::deque<UInt32> fire_signal;
    std::list<std::weak_ptr<WindowViewBlockInputStream>> watch_streams;
    std::condition_variable_any fire_signal_condition;
    std::condition_variable fire_condition;
    BlocksListPtrs mergeable_blocks;

    /// Mutex for the blocks and ready condition
    std::mutex mutex;
    std::mutex flush_table_mutex;
    std::shared_mutex fire_signal_mutex;

    IntervalKind::Kind window_kind;
    IntervalKind::Kind hop_kind;
    IntervalKind::Kind watermark_kind;
    Int64 window_num_units;
    Int64 hop_num_units;
    Int64 watermark_num_units = 0;
    String window_column_name;
    String timestamp_column_name;

    StorageID select_table_id = StorageID::createEmpty();
    StorageID target_table_id = StorageID::createEmpty();
    StorageID inner_table_id = StorageID::createEmpty();
    StoragePtr parent_storage;
    StoragePtr inner_storage;
    StoragePtr target_storage;

    BackgroundSchedulePool::TaskHolder cleanCacheTask;
    BackgroundSchedulePool::TaskHolder fireTask;

    ExpressionActionsPtr writeExpressions;

    ASTPtr innerQueryParser(ASTSelectQuery & inner_query);

    std::shared_ptr<ASTCreateQuery> generateInnerTableCreateQuery(const ASTCreateQuery & inner_create_query, const String & database_name, const String & table_name);

    UInt32 getWindowLowerBound(UInt32 time_sec, int window_id_skew = 0);
    UInt32 getWindowUpperBound(UInt32 time_sec, int window_id_skew = 0);

    void fire(UInt32 watermark);
    void cleanCache();
    void threadFuncCleanCache();
    void threadFuncFireProc();
    void threadFuncFireEvent();
    void addFireSignal(std::deque<UInt32> & signals);
    void updateMaxWatermark(UInt32 watermark);
    void updateMaxTimestamp(UInt32 timestamp);

    static Pipes blocksToPipes(BlocksListPtrs & blocks, Block & sample_block);

    ASTPtr getInnerQuery() const { return inner_query->clone(); }
    ASTPtr getFinalQuery() const { return final_query->clone(); }

    StoragePtr getParentStorage()
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

    StoragePtr& getTargetStorage()
    {
        if (target_storage == nullptr && !target_table_id.empty())
            target_storage = global_context.getTable(target_table_id);
        return target_storage;
    }

    Block & getHeader()
    {
        if (!sample_block)
        {
            sample_block = InterpreterSelectQuery(
                               getInnerQuery(), global_context, getParentStorage(), SelectQueryOptions(QueryProcessingStage::Complete))
                               .getSampleBlock();
            for (size_t i = 0; i < sample_block.columns(); ++i)
                sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
        }
        return sample_block;
    }

    Block & getMergeableHeader()
    {
        if (!mergeable_sample_block)
        {
            mergeable_sample_block = mergeable_blocks->front()->front().cloneEmpty();
        }
        return mergeable_sample_block;
    }

    StorageWindowView(
        const StorageID & table_id_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns,
        bool attach_);
};
}
