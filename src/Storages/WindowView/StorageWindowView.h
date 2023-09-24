#pragma once

#include <Common/SharedMutex.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeInterval.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>
#include <Poco/Logger.h>

#include <mutex>

namespace DB
{
class IAST;
class WindowViewSource;
using ASTPtr = std::shared_ptr<IAST>;

/**
 * StorageWindowView.
 *
 * CREATE WINDOW VIEW [IF NOT EXISTS] [db.]name [TO [db.]name]
 * [INNER ENGINE engine] [ENGINE engine]
 * [WATERMARK strategy] [ALLOWED_LATENESS interval_function]
 * [POPULATE]
 * AS SELECT ...
 * GROUP BY [tumble/hop(...)]
 *
 * - only stores data that has not been triggered yet;
 * - fire_task checks if there is a window ready to be fired
 *   (each window result is fired in one output at the end of tumble/hop window interval);
 * - intermediate data is stored in inner table with
 *   AggregatingMergeTree engine by default, but any other -MergeTree
 *   engine might be used as inner table engine;
 * - WATCH query is supported (WATCH [db.]name [LIMIT n]);
 *
 *   Here function in GROUP BY clause results in a "window_id"
 *   represented as Tuple(DateTime, DateTime) - lower and upper bounds of the window.
 *   Function might be one of the following:
 *     1. tumble(time_attr, interval [, timezone])
 *        - non-overlapping, continuous windows with a fixed duration (interval);
 *        - example:
 *            SELECT tumble(toDateTime('2021-01-01 00:01:45'), INTERVAL 10 SECOND)
 *            results in ('2021-01-01 00:01:40','2021-01-01 00:01:50')
 *     2. hop(time_attr, hop_interval, window_interval [, timezone])
 *        - sliding window;
 *        - has a fixed duration (window_interval parameter) and hops by a
 *          specified hop interval (hop_interval parameter);
 *          If the hop_interval is smaller than the window_interval, hopping windows
 *          are overlapping. Thus, records can be assigned to multiple windows.
 *        - example:
 *            SELECT hop(toDateTime('2021-01-01 00:00:45'), INTERVAL 3 SECOND, INTERVAL 10 SECOND)
 *            results in ('2021-01-01 00:00:38','2021-01-01 00:00:48')
 *
 *   DateTime value can be used with the following functions to find out start/end of the window:
 *     - tumbleStart(time_attr, interval [, timezone]), tumbleEnd(time_attr, interval [, timezone])
 *     - hopStart(time_attr, hop_interval, window_interval [, timezone]), hopEnd(time_attr, hop_interval, window_interval [, timezone])
 *
 *
 * Time processing options.
 *
 *   1. (default) processing time
 *      - produces results based on the time of the local machine;
 *      - example:
 *          CREATE WINDOW VIEW test.wv TO test.dst
 *          AS SELECT count(number), tumbleStart(w_id) as w_start FROM test.mt
 *          GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
 *
 *   2. event time
 *      - produces results based on the time that is contained in every record;
 *      - event time processing is implemented by using WATERMARK:
 *         a. STRICTLY_ASCENDING
 *            - emits a watermark of the maximum observed timestamp so far;
 *            - rows that have a timestamp < max timestamp are not late.
 *         b. ASCENDING
 *            - rows that have a timestamp <= max timestamp are not late.
 *         c. BOUNDED (WATERMARK = INTERVAL)
 *            - emits watermarks, which are the maximum observed timestamp minus
 *              the specified delay.
 *      - example:
 *          CREATE WINDOW VIEW test.wv TO test.dst
 *          WATERMARK=STRICTLY_ASCENDING
 *          AS SELECT count(number) FROM test.mt
 *          GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
 *        (where `timestamp` is a DateTime column in test.mt)
 *
 *
 * Lateness.
 *   - By default, the allowed lateness is set to off, that is, elements that arrive
 *     behind the watermark will be dropped.
 *
 *   - Can be enabled by using ALLOWED_LATENESS=INTERVAL, like this:
 *       CREATE WINDOW VIEW test.wv TO test.dst
 *       WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND
 *       AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt
 *       GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
 *
 *   - Instead of firing at the end of windows, WINDOW VIEW will fire
 *     immediately when encountering late events;
 *     Thus, it will result in multiple outputs for the same window.
 *     Users need to take these duplicated results into account.
 */

class StorageWindowView final : public IStorage, WithContext
{
    friend class WindowViewSource;
    friend class WatermarkTransform;

public:
    StorageWindowView(
        const StorageID & table_id_,
        ContextPtr context_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);

    String getName() const override { return "WindowView"; }

    bool isView() const override { return true; }
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override;

    void dropInnerTableIfAny(bool sync, ContextPtr context) override;

    void drop() override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    void startup() override;
    void shutdown() override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    Pipe watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    std::pair<BlocksPtr, Block> getNewBlocks(UInt32 watermark);

    BlockIO populate();

    static void writeIntoWindowView(StorageWindowView & window_view, const Block & block, ContextPtr context);

    ASTPtr getMergeableQuery() const { return mergeable_query->clone(); }

    ASTPtr getSourceTableSelectQuery();

    Block getInputHeader() const;

    const Block & getOutputHeader() const;

private:
    Poco::Logger * log;

    /// Stored query, e.g. SELECT * FROM * GROUP BY tumble(now(), *)
    ASTPtr select_query;
    /// Used to generate the mergeable state of select_query, e.g. SELECT * FROM * GROUP BY windowID(____timestamp, *)
    ASTPtr mergeable_query;
    /// Used to fetch the mergeable state and generate the final result. e.g. SELECT * FROM * GROUP BY tumble(____timestamp, *)
    ASTPtr final_query;
    /// Used to fetch the data from inner storage.
    ASTPtr inner_fetch_query;

    bool is_proctime;
    bool is_time_column_func_now;
    bool is_tumble; // false if is hop
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> modifying_query{false};
    bool has_inner_table{true};
    bool has_inner_target_table{false};
    mutable Block output_header;
    UInt64 fire_signal_timeout_s;
    UInt64 clean_interval_usec;
    UInt64 last_clean_timestamp_usec = 0;
    const DateLUTImpl * time_zone = nullptr;
    UInt32 max_timestamp = 0;
    UInt32 max_watermark = 0; // next watermark to fire
    UInt32 max_fired_watermark = 0;
    bool is_watermark_strictly_ascending;
    bool is_watermark_ascending;
    bool is_watermark_bounded;
    bool allowed_lateness;
    UInt32 next_fire_signal;
    std::deque<UInt32> fire_signal;
    std::list<std::weak_ptr<WindowViewSource>> watch_streams;
    std::condition_variable_any fire_signal_condition;
    std::condition_variable fire_condition;

    /// Mutex for the blocks and ready condition
    std::mutex mutex;
    SharedMutex fire_signal_mutex;
    mutable std::mutex sample_block_lock; /// Mutex to protect access to sample block

    IntervalKind::Kind window_kind;
    IntervalKind::Kind hop_kind;
    IntervalKind::Kind watermark_kind;
    IntervalKind::Kind lateness_kind;
    IntervalKind::Kind slide_kind;
    Int64 window_num_units;
    Int64 hop_num_units;
    Int64 slice_num_units;
    Int64 watermark_num_units;
    Int64 lateness_num_units;
    Int64 slide_num_units;
    String window_id_name;
    String window_id_alias;
    String window_column_name;
    String timestamp_column_name;

    StorageID select_table_id = StorageID::createEmpty();
    StorageID target_table_id = StorageID::createEmpty();
    StorageID inner_table_id = StorageID::createEmpty();

    ASTPtr inner_table_engine;

    BackgroundSchedulePool::TaskHolder clean_cache_task;
    BackgroundSchedulePool::TaskHolder fire_task;

    String window_view_timezone;
    String function_now_timezone;

    ASTPtr innerQueryParser(const ASTSelectQuery & query);
    void eventTimeParser(const ASTCreateQuery & query);
    ASTPtr initInnerQuery(ASTSelectQuery query, ContextPtr context);

    UInt32 getCleanupBound();
    ASTPtr getCleanupQuery();

    UInt32 getWindowLowerBound(UInt32 time_sec);
    UInt32 getWindowUpperBound(UInt32 time_sec);

    void fire(UInt32 watermark);
    void cleanup();
    void threadFuncCleanup();
    void threadFuncFireProc();
    void threadFuncFireEvent();
    void addFireSignal(std::set<UInt32> & signals);
    void updateMaxWatermark(UInt32 watermark);
    void updateMaxTimestamp(UInt32 timestamp);

    ASTPtr getFinalQuery() const { return final_query->clone(); }
    ASTPtr getInnerTableCreateQuery(const ASTPtr & inner_query, const StorageID & inner_table_id);

    StoragePtr getSourceTable() const;
    StoragePtr getInnerTable() const;
    StoragePtr getTargetTable() const;
};
}
