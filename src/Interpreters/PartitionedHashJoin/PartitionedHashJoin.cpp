#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Common/FailPoint.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/SharedFixedHashTableFilter.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <iterator>
#include <mutex>

namespace CurrentMetrics
{
extern const Metric HashJoinDestroyThreads;
extern const Metric HashJoinDestroyThreadsActive;
extern const Metric HashJoinDestroyThreadsScheduled;
}

namespace ProfileEvents
{
extern const Event HashJoinPartitionedBuildMicroseconds;
extern const Event HashJoinPartitionedBuildFillMicroseconds;
extern const Event HashJoinPartitionedBuildInsertMicroseconds;
extern const Event HashJoinPartitionedProbeMicroseconds;
extern const Event HashJoinPartitions;
extern const Event HashJoinTableBytes;
extern const Event HashJoinPartitionOverflowRows;
extern const Event HashJoinDuplicateRunBytes;
extern const Event HashJoinTeardownMicroseconds;
extern const Event HashJoinRowStoreBlocks;
extern const Event HashJoinPreallocatedElementsInHashTables;
}

namespace DB
{

namespace ErrorCodes
{
extern const int FAULT_INJECTED;
extern const int INCOMPATIBLE_TYPE_OF_JOIN;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace FailPoints
{
extern const char hash_join_throw_after_data_release[];
}

namespace
{

/// `joinBlock` only sets up a lazy result - the matching runs inside `IJoinResult::next` - so the
/// probe time has to be accounted there.
class TimedJoinResult : public IJoinResult
{
public:
    TimedJoinResult(JoinResultPtr result_, ProfileEvents::Event event_)
        : result(std::move(result_))
        , event(event_)
    {
    }

    JoinResultBlock next() override
    {
        ProfileEventTimeIncrement<Microseconds> watch(event);
        return result->next();
    }

    /// The transform sums this into the count `onProbePhaseFinish` receives. Without it the join would
    /// publish zero matches. The planner would then never enable the row store for the next run.
    std::optional<size_t> getMatchedRightRows() const override { return result->getMatchedRightRows(); }

private:
    JoinResultPtr result;
    ProfileEvents::Event event;
};

}

PartitionedHashJoin::PartitionedHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    size_t num_threads_,
    bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    size_t max_bytes_before_external_join_,
    std::optional<size_t> build_rows_hint_)
    : PartitionedHashJoin(
          std::move(table_join_),
          std::move(right_sample_block_),
          num_threads_,
          any_take_last_row_,
          stats_collecting_params_,
          max_bytes_before_external_join_,
          build_rows_hint_,
          /*join_table_mode_=*/false)
{
}

PartitionedHashJoin::PartitionedHashJoin(
    JoinTableTag, std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_)
    : PartitionedHashJoin(
          std::move(table_join_),
          std::move(right_sample_block_),
          /*num_threads_=*/1,
          any_take_last_row_,
          HashJoinStatsCollectingParams{},
          /*max_bytes_before_external_join_=*/0,
          /*build_rows_hint_=*/std::nullopt,
          /*join_table_mode_=*/true)
{
}

PartitionedHashJoin::PartitionedHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    size_t num_threads_,
    bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    size_t max_bytes_before_external_join_,
    std::optional<size_t> build_rows_hint_,
    bool join_table_mode_)
    : table_join(std::move(table_join_))
    , right_sample_block(std::move(right_sample_block_))
    , any_take_last_row(any_take_last_row_)
    , num_threads(std::max<size_t>(1, num_threads_))
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , hash_join(std::make_unique<HashJoin>(table_join, right_sample_block, any_take_last_row, /*allow_set_maps_=*/false))
    , join_table_mode(join_table_mode_)
    , used_flags_per_row(hash_join->needUsedFlagsForPerRightTableRow(table_join))
    , cached_distinct_estimates(table_join->getClauses().size())
    , live_merged_hll(table_join->getClauses().size())
    , build_rows_hint(build_rows_hint_)
    , single_fill_thread(
          !join_table_mode && (num_threads == 1 || (build_rows_hint_ && *build_rows_hint_ < table_join->parallelHashJoinThreshold())))
    , stats_collecting_params(stats_collecting_params_.build)
    , match_stats_collecting_params(stats_collecting_params_.match)
    , log(getLogger("PartitionedHashJoin"))
{
    if (!HashJoinTableMaps::isSupportedType(hash_join->data->type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin was created for an unsupported map type {}; the plan-time gate must reject this shape",
            hash_join->data->type);

    for (size_t clause_idx = 0; clause_idx < table_join->getClauses().size(); ++clause_idx)
        clauses.emplace_back(
            *hash_join, *table_join, clause_idx, any_take_last_row, num_threads, max_bytes_before_external_join, build_blocks, accumulated_bytes, log);
    post_build_pools.resize(clauses.size());

    /// The same shapes for which `HashJoin` allocates its per-row flags: the flagged (kind, strictness)
    /// pairs of `MapGetter`. The others mark nothing and read nothing.
    if (used_flags_per_row)
        joinDispatch(
            hash_join->getKind(),
            hash_join->getStrictness(),
            hash_join->data->maps.front(),
            hash_join->getMapsKind(),
            [&](auto kind_, auto strictness_, auto & map_)
            { allocate_per_row_flags = MapGetter<kind_, strictness_, mapsKindOf<decltype(map_)>()>::flagged; });

    /// `HashJoin`'s constructor derived the row store layout as `hash` builds it, so it gave way to the
    /// rerange optimization. This join never reranges its rows: derive the layout without that rule.
    if (hash_join->data->row_store_state == HashJoin::RowStoreState::Disabled && table_join->isRowStoreEnabled()
        && hash_join->isRowStoreSupported() && hash_join->isRightTableRerangeEnabled())
    {
        hash_join->data->row_store_state = HashJoin::RowStoreState::Enabled;
        hash_join->initRowStore(hash_join->data->sample_block, /*may_rerange=*/false);
    }

    /// Sized once and never resized, because the lock-free paths index them without synchronizing
    /// against growth. Twice the thread count leaves room for pipelines with more transforms than
    /// threads; a lane index past the table takes the mutexed fallback.
    fill_lane_slots = std::vector<std::atomic<FillLane *>>(2 * num_threads);
    probe_scratch_slots = std::vector<std::atomic<ProbeScratch *>>(2 * num_threads);

    if (table_join->collectAnalyzeStats())
        matched_rows_stats = std::make_unique<MatchedRowsStats>(hash_join->getKind(), hash_join->getStrictness(), table_join->analyzeMode());

    if (join_table_mode)
    {
        /// `StorageJoin` accepts one key clause, no mixed ON condition and no ASOF at `CREATE`, so these
        /// are not user errors.
        if (clauses.size() != 1 || used_flags_per_row)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: a Join table has exactly one key clause and no mixed ON condition");
        if (hash_join->getStrictness() == JoinStrictness::Asof)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: a Join table cannot be ASOF");
        clauses.front().createJoinTable();
    }
    else if (!single_fill_thread && max_bytes_before_external_join == 0 && table_join->sizeLimits().max_rows == 0)
        readDistinctKeysFromStatisticsCache();

    /// Charge the constructor's allocations before the plan creates another join.
    CurrentThread::flushUntrackedMemory();
    CurrentMemoryTracker::check();
}

PartitionedHashJoin::~PartitionedHashJoin()
{
    /// Table first: cells point into the arenas and the row store.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinTeardownMicroseconds);

    /// Other hash joins publish the matched count from their destructors too.
    if (build_phase_finished && probe_phase_finished && hash_table_matches.has_value()
        && match_stats_collecting_params.isCollectionAndUseEnabled())
    {
        try
        {
            getHashTablesStatistics<HashJoinMatchEntry>().update({.matches = *hash_table_matches}, match_stats_collecting_params);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    for (auto & clause : clauses)
        clause.releaseTable();
    destroyStoredBlocksInParallel();
    hash_join.reset();
    probe_scratch_pool.clear();
    for (auto & slot : probe_scratch_slots)
        delete slot.load(std::memory_order_acquire);
}

void PartitionedHashJoin::destroyStoredBlocksInParallel()
{
    /// The table went with `clause.releaseTable()`, and the arena is one allocation. What is left to
    /// free is the stored right blocks: hundreds of megabytes to gigabytes on a large build. Several
    /// threads free them. A Join table's per-query instances share the storage's `data`, so only its
    /// last owner destroys it.
    static constexpr size_t PARALLEL_DESTROY_THRESHOLD_BYTES = 100 * 1024 * 1024;

    if (num_threads <= 1 || !hash_join->data || hash_join->data.use_count() != 1
        || hash_join->data->allocated_size.load(std::memory_order_relaxed) < PARALLEL_DESTROY_THRESHOLD_BYTES)
        return;

    try
    {
        HashJoin::StoredBlocksList blocks = std::move(storedBlocks());
        const size_t num_tasks = std::min(num_threads, blocks.size());
        if (num_tasks <= 1)
            return;

        /// Runs of consecutive blocks spliced off the one list: a list node keeps its address, and no cell
        /// refers to a block any more.
        std::vector<HashJoin::StoredBlocksList> slices(num_tasks);
        const size_t blocks_per_task = (blocks.size() + num_tasks - 1) / num_tasks;
        for (auto & slice : slices)
        {
            auto end = blocks.begin();
            std::advance(end, std::min(blocks_per_task, blocks.size()));
            slice.splice(slice.end(), blocks, blocks.begin(), end);
        }

        ThreadPool pool(
            CurrentMetrics::HashJoinDestroyThreads,
            CurrentMetrics::HashJoinDestroyThreadsActive,
            CurrentMetrics::HashJoinDestroyThreadsScheduled,
            num_tasks);
        for (auto & slice : slices)
            pool.scheduleOrThrowOnError(
                [&slice, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::HASH_JOIN_DESTRUCTION);
                    slice.clear();
                });
        pool.wait();
    }
    catch (...)
    {
        /// Unscheduled blocks are freed when the local lists unwind.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool PartitionedHashJoin::isSupported(const TableJoin & table_join)
{
    /// Everything `HashJoin` serves: INNER, LEFT, RIGHT and FULL with ALL, ANY, RightAny, SEMI, ANTI
    /// and ASOF. Null maps, ON filters, mixed non-equi conditions, USING, several disjuncts and a Join
    /// table or a key-value storage on the right side all pass. Cross, Comma, Paste and ON-constant
    /// joins are routed before the algorithm loop. The ASOF shapes `HashJoin` rejects (not INNER or
    /// LEFT, or without an equality key) throw its errors from the constructor, because the inner
    /// `HashJoin` is built first. A memory limit is no reason to decline: the planner wraps this join
    /// in `SpillingHashJoin` instead.
    const JoinKind kind = table_join.kind();
    const JoinStrictness strictness = table_join.strictness();

    if (!isInner(kind) && !isLeft(kind) && !isRight(kind) && !isFull(kind))
        return false;

    switch (strictness)
    {
        case JoinStrictness::All:
        case JoinStrictness::Any:
        case JoinStrictness::RightAny:
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
        case JoinStrictness::Asof: return true;
        default: return false;
    }
}

const TableJoin & PartitionedHashJoin::getTableJoin() const
{
    return *table_join;
}

void PartitionedHashJoin::shareJoinTable(const PartitionedHashJoin & source)
{
    if (!join_table_mode || !source.join_table_mode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: only the instances of a Join table share its table");
    /// `StorageJoin` checked the kind and strictness; the map type follows from the key columns, which
    /// are the storage's. Both have to agree, or the probe would read the cells through the wrong layout.
    if (clauses.front().mapsVariantIndex() != source.clauses.front().mapsVariantIndex() || hash_join->data->type != source.hash_join->data->type)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin: the query's join shape (maps {}, type {}) does not match the Join table's (maps {}, type {})",
            clauses.front().mapsVariantIndex(),
            hash_join->data->type,
            source.clauses.front().mapsVariantIndex(),
            source.hash_join->data->type);

    hash_join->reuseJoinedData(*source.hash_join);
    clauses.front().shareTable(source.clauses.front());
    shared_from_join_table = true;
    /// `reuseJoinedData` sized the flags to the inner join's own, empty map.
    reinitUsedFlags();
    /// The stored blocks are the storage's now, so the right-side flags of the statistics can be
    /// sized to them, as `HashJoin::reuseJoinedData` sizes its own.
    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(storedBlocks());
}

DataTypePtr PartitionedHashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    return hash_join->joinGetCheckAndGetReturnType(data_types, column_name, or_null);
}

ColumnWithTypeAndName PartitionedHashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add)
{
    const JoinStrictness strictness = hash_join->getStrictness();
    const bool is_valid = (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny) && hash_join->getKind() == JoinKind::Left;
    if (!is_valid)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "joinGet only supports StorageJoin of type Left Any");

    /// The keys under the storage's names, which is how the probe reads a right-side key.
    const auto & key_names_right = table_join->getOnlyClause().key_names_right;
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    /// Concurrent `joinGet` calls probe the storage's instance under its read lock; a flagged shape
    /// would write the shared used flags.
    static_assert(
        !MapGetter<JoinKind::Left, JoinStrictness::Any, JoinMapsKind::Default>::flagged,
        "joinGet is not protected from hash table changes between block processing");

    auto result = probeImpl<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsOne>(std::move(keys), invalid_lane, &block_with_columns_to_add);
    auto res = result->next();
    chassert(res.is_last);
    return res.block.getByPosition(res.block.columns() - 1);
}

void PartitionedHashJoin::shrinkStoredBlocksToFit()
{
    size_t total_bytes = getTotalByteCount();
    hash_join->shrinkStoredBlocksToFit(total_bytes, /*force_optimize=*/true);
}

PartitionedHashJoin::FillLane & PartitionedHashJoin::getFillLane()
{
    std::lock_guard lock(fill_mutex);
    auto [it, inserted] = lane_by_thread.try_emplace(std::this_thread::get_id(), nullptr);
    if (inserted)
        it->second = &lanes.emplace_back(clauses.size());
    return *it->second;
}

PartitionedHashJoin::FillLane & PartitionedHashJoin::getFillLane(size_t worker_id)
{
    if (worker_id >= fill_lane_slots.size())
        return getFillLane();

    if (FillLane * fast = fill_lane_slots[worker_id].load(std::memory_order_acquire))
        return *fast;

    /// First block of this lane: one mutexed emplace into the deque, whose elements are stable, and
    /// every later block takes the atomic load above. A worker id is unique per filling transform
    /// and a transform's work is serialized. The slot is therefore single-writer once published,
    /// even though executor threads migrate between transforms.
    std::lock_guard lock(fill_mutex);
    if (FillLane * raced = fill_lane_slots[worker_id].load(std::memory_order_relaxed))
        return *raced;
    FillLane * fresh = &lanes.emplace_back(clauses.size());
    fill_lane_slots[worker_id].store(fresh, std::memory_order_release);
    return *fresh;
}

bool PartitionedHashJoin::addBlockToJoin(const Block & source_block, size_t /*num_rows*/, size_t worker_id, bool check_limits)
{
    /// `num_rows` only matters for the columnless CROSS blocks this algorithm never plans.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (build_phase_finished || stored_blocks_released)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: addBlockToJoin called after the build phase finished");

    /// Key preparation plus the per-row hash, route and sketch update. The partition plan comes later,
    /// at the barrier, so every plan pays exactly this much here.
    ProfileEventTimeIncrement<Microseconds> fill_watch(ProfileEvents::HashJoinPartitionedBuildFillMicroseconds);

    Block materialized = hash_join->materializeColumnsFromRightBlock(source_block);
    const size_t rows = materialized.rows();
    if (rows == 0)
        return true;

    /// `RowRef::row_no` is 32-bit, as in `HashJoin`.
    if (rows > std::numeric_limits<UInt32>::max()) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for PartitionedHashJoin: {}", rows);

    FillBlock fill;
    fill.rows = rows;
    /// Zeroed here, on the fill thread, rather than for every block at once on the thread that stores
    /// them; `HashJoin` allocates its per-row flags on its fill workers too.
    if (allocate_per_row_flags)
        fill.per_row_flags = JoinStuff::JoinUsedFlags::UsedFlagsForColumns(rows);

    fill.clauses.resize(clauses.size());
    for (const auto & clause : clauses)
        clause.prepareInput(materialized, fill);

    /// The payload in stored form. The constructor already decided the row store layout. The columns
    /// it admits are packed row-wise here. The probe then reads one row pointer per output row,
    /// instead of one random column read per output column. The remaining columns stay columnar.
    Block prepared = HashJoin::prepareRightBlock(materialized, hash_join->savedBlockSample());
    assertBlocksHaveEqualStructureAllowReplicated(hash_join->data->sample_block, prepared, "joined block");
    fill.stored = hash_join->createStoredBlock(prepared, ScatteredBlock::Selector(rows));

    if (join_table_mode)
    {
        /// One block at a time under the storage's write lock: stored, then inserted straight into the
        /// table, which is probe-ready again when this returns. No routes, no sketch, no barrier. The
        /// limits are the storage's `max_rows_in_join` / `max_bytes_in_join`, checked as `HashJoin` does.
        const bool nullmap_saved = storeBlockInRowStore(fill);
        const bool any_row_stored = clauses.front().insertJoinTableBlock(fill);
        if (!any_row_stored && !nullmap_saved)
            dropLastStoredBlock();

        if (!check_limits)
            return true;
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }

    if (single_fill_thread)
    {
        /// One fill thread and no partition plan, so no routes and no sketch. The block is stored and
        /// its rows go into the table right away. The table grows like `hash`'s when it was sized low.
        /// The fill block stays alive through the insert because its key pointers point into its holders.
        accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
        accumulated_bytes.fetch_add(fill.stored.allocatedBytes(), std::memory_order_relaxed);
        storeBlockInRowStore(fill);
        /// The table is sized before the first block from a distinct-key count, never from the row
        /// hint alone: the hint counts rows, and a build with many rows per key would get a table
        /// oversized by that multiplicity. The count is the one a previous run of this query left in
        /// the hash table statistics cache. Without one the table starts at the smallest degree and
        /// doubles as the keys arrive, as `hash`'s does. The row hint only caps the reserve: a table
        /// cannot hold more keys than rows. Every clause sizes its table from the one cached count.
        if (!clauses.front().hasTable())
        {
            const size_t keys = readDistinctKeysFromStatisticsCache() ? *cached_distinct_keys : 1;
            for (auto & clause : clauses)
                clause.beginSinglePartitionInsert(
                    clause.reserveFor(build_rows_hint.value_or(keys), static_cast<double>(keys)),
                    accumulated_rows.load(std::memory_order_relaxed),
                    /*grow_at_max_fill_=*/true);
        }
        for (auto & clause : clauses)
            clause.insertSingleLaneBlock(fill);

        return !check_limits || checkFillLimits();
    }

    FillLane & lane = getFillLane(worker_id);
    if (max_bytes_before_external_join || table_join->sizeLimits().max_rows)
    {
        /// A sketch merge waits for this block's rank updates before reading the lane.
        /// One hash pass per clause, as `HashJoin` hashes each block once per map.
        std::lock_guard hll_lock(lane.hll_mutex);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
            clauses[clause_idx].computeRoutes(fill, lane.hll[clause_idx]);
    }
    else if (cached_distinct_keys)
    {
        for (const auto & clause : clauses)
            clause.computeRoutes(fill);
    }
    else
    {
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
            clauses[clause_idx].computeRoutes(fill, lane.hll[clause_idx]);
    }

    accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
    accumulated_bytes.fetch_add(fill.stored.allocatedBytes() + fill.routeBytes(), std::memory_order_relaxed);
    lane.blocks.push_back(std::move(fill));

    return !check_limits || checkFillLimits();
}

size_t PartitionedHashJoin::rowCountForLimit(size_t max_rows) const
{
    /// One clause has at most one distinct key per input row. Merging its lane sketches is unnecessary
    /// until that bound reaches the limit. Multiple clauses count their keys separately.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    if (max_rows == 0 || (clauses.size() == 1 && rows < max_rows))
        return rows;
    return getTotalRowCount();
}

bool PartitionedHashJoin::checkFillLimits()
{
    /// The parallel fill estimates the distinct keys; the exact count is checked after the build.
    const SizeLimits & limits = table_join->sizeLimits();
    if (!limits.hasLimits())
        return true;
    limits_requested = true;
    return limits.check(rowCountForLimit(limits.max_rows), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void PartitionedHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_join->checkTypesOfKeys(block);
}

void PartitionedHashJoin::setTotals(const Block & block)
{
    if (!block.empty())
    {
        std::lock_guard lock(totals_mutex);
        totals = block;
    }
}

const Block & PartitionedHashJoin::getTotals() const
{
    return totals;
}

bool PartitionedHashJoin::storeBlockInRowStore(FillBlock & fill)
{
    auto & data = *hash_join->data;
    /// Registered and accounted while a local list still owns it, as `HashJoin::addBlockToJoin` does:
    /// a registration that throws leaves no unregistered block behind, and `splice` cannot throw. A
    /// list node keeps its address across the splice.
    HashJoin::StoredBlocksList new_block;
    new_block.push_back(std::move(fill.stored));
    StoredBlock & stored = new_block.back();
    stored.block_no = data.stored_columns_index->add(&stored);
    data.addBytes(data.allocated_size, stored.allocatedBytes());
    data.rows_to_join.fetch_add(fill.rows, std::memory_order_relaxed);
    storedBlocks().splice(storedBlocks().end(), new_block);
    fill.block_no = stored.block_no;
    fill.stored = StoredBlock{};
    if (stored.hasRowStore())
    {
        ++row_store_blocks;
        ProfileEvents::increment(ProfileEvents::HashJoinRowStoreBlocks);
    }

    /// Per-row used flags cover every stored row, the ones that never enter a table included. A row
    /// nothing marks is emitted as non-joined, so no null map is kept for it; `HashJoin` keeps none either.
    /// The flags are attached here, on the one thread that stores blocks, before any probe can read them.
    if (allocate_per_row_flags)
    {
        auto & flags = hash_join->used_flags->per_row_flags;
        if (flags.size() <= stored.block_no)
            flags.resize(stored.block_no + 1);
        flags[stored.block_no] = std::move(fill.per_row_flags);
    }
    if (!isRightOrFull(hash_join->getKind()) || used_flags_per_row)
        return false;

    /// RIGHT/FULL output needs the rows that never made it into the table - null keys and rows the
    /// ON condition filtered - exactly as the standard build saves them, from the one clause's null map
    /// and mask.
    HashJoinClause::Input & input = fill.clauses.front();
    bool save_nullmap = false;
    if (input.null_map)
        for (size_t i = 0; i < fill.rows && !save_nullmap; ++i)
            save_nullmap = (*input.null_map)[i];
    bool nullmap_saved = false;
    if (save_nullmap)
    {
        auto & holder = storedNullmaps().emplace_back(&stored, input.null_map_holder);
        data.addBytes(data.nullmaps_allocated_size, holder.allocatedBytes());
        nullmap_saved = true;
    }

    if (input.join_mask.hasData() && input.join_mask.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
    {
        auto not_joined_map = ColumnUInt8::create(fill.rows, static_cast<UInt8>(0));
        bool has_right_not_joined = false;
        for (size_t i = 0; i < fill.rows; ++i)
        {
            if (!input.join_mask.isRowFiltered(i))
                continue;
            if (save_nullmap && (*input.null_map)[i])
                continue; /// already covered by the null-keys map
            not_joined_map->getData()[i] = 1;
            has_right_not_joined = true;
        }
        if (has_right_not_joined)
        {
            auto & holder = storedNullmaps().emplace_back(&stored, std::move(not_joined_map));
            data.addBytes(data.nullmaps_allocated_size, holder.allocatedBytes());
            nullmap_saved = true;
        }
    }
    return nullmap_saved;
}

void PartitionedHashJoin::dropLastStoredBlock()
{
    /// The engine stores one block at a time, so the block just stored is the list's last. Its rows
    /// went through the insert, so the key count stands; the row count and the bytes must not include it.
    auto & data = *hash_join->data;
    StoredBlock & stored = storedBlocks().back();
    data.subBytes(data.allocated_size, stored.allocatedBytes());
    data.rows_to_join.fetch_sub(stored.selector.size(), std::memory_order_relaxed);
    /// No cell refers to the block; the nulled entry makes a stale ref fail loudly.
    data.stored_columns_index->clearEntry(stored.block_no);
    storedBlocks().pop_back();
}

void PartitionedHashJoin::onBuildPhaseFinish()
{
    /// A Join table's join is built at every point: the storage inserts under its write lock and the
    /// per-query instances only probe. There is nothing to finish.
    if (join_table_mode)
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (single_fill_thread)
    {
        /// Everything was inserted as it arrived. A build that never saw a block still needs its tables,
        /// because the used flags and the probe are sized from them. The exact distinct count stands in
        /// for the sketch estimate the memory gate reads.
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            auto & clause = clauses[clause_idx];
            if (!clause.hasTable())
                clause.beginSinglePartitionInsert(
                    clause.reserveFor(1, 1.0), accumulated_rows.load(std::memory_order_relaxed), /*grow_at_max_fill_=*/true);
            clause.setDistinctEstimate(static_cast<double>(clause.claimedTotal()));
            ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clause.partitionCount());
            LOG_TRACE(
                log,
                "Single fill thread: table of clause {} has 2^{} cells, {} rows in {} blocks inserted during the fill, {} distinct keys",
                clause_idx,
                clause.sizeDegree(),
                accumulated_rows.load(std::memory_order_relaxed),
                storedBlocks().size(),
                static_cast<size_t>(clause.hllEstimate()));
        }
        return;
    }

    /// Run once by the last fill thread, and deliberately cheap: concatenate the lanes, number the
    /// row-store blocks, merge the sketches, pick the plan. The scatter, allocation and inserts are
    /// `runPostBuildPhase`'s work. Every fill call has returned, so the lane locks are free.
    std::vector<DenseHyperLogLog> merged(clauses.size());
    size_t total_blocks = 0;
    {
        std::lock_guard lock(fill_mutex);
        for (const auto & lane : lanes)
            total_blocks += lane.blocks.size();
        build_blocks.reserve(total_blocks);
        for (auto & lane : lanes)
        {
            if (!cached_distinct_keys)
            {
                std::lock_guard hll_lock(lane.hll_mutex);
                for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
                    merged[clause_idx].merge(lane.hll[clause_idx]);
            }
            for (auto & block : lane.blocks)
                build_blocks.push_back(std::move(block));
            lane.blocks.clear();
        }
        lanes.clear();
        lane_by_thread.clear();
    }

    /// A previous run's exact count replaces the sketch estimate. The table it sizes needs no safety
    /// margin and, when the data has not changed, no grow: the preallocation `HashJoin` made from the
    /// same cache entry. The entry counts the keys of every clause together, and each clause sizes from
    /// it. `HashJoin` reserves each of its maps from the one entry the same way.
    if (max_bytes_before_external_join || table_join->sizeLimits().max_rows)
        readDistinctKeysFromStatisticsCache();
    const bool exact = cached_distinct_keys.has_value();
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        clauses[clause_idx].setDistinctEstimate(exact ? static_cast<double>(*cached_distinct_keys) : merged[clause_idx].estimate(), exact);
    for (auto & fill : build_blocks)
        storeBlockInRowStore(fill);
    /// Every estimate is set before the first plan: a clause's partition floor is guarded by what the
    /// other clauses' tables will take.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
        clauses[clause_idx].decidePartitionPlan(rows);
        ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clauses[clause_idx].partitionCount());
    }
}

PartitionedHashJoin::PostBuildPlan PartitionedHashJoin::planPostBuild()
{
    if (max_bytes_before_external_join == 0)
        return PostBuildPlan::Fits;

    /// Everything is already resident: the stored blocks, the table and the duplicate runs.
    if (single_fill_thread)
        return getTotalByteCount() <= max_bytes_before_external_join ? PostBuildPlan::Fits : PostBuildPlan::MustSpill;

    /// The worst verdict over the clauses wins: `MustSpill` over `Grouped` over `Fits`, the enum's order.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    PostBuildPlan plan = PostBuildPlan::Fits;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
        plan = std::max(plan, clauses[clause_idx].planPostBuild(rows, postBuildPool(clause_idx)));
    }
    return plan;
}

void PartitionedHashJoin::runPostBuildPhase()
{
    if (join_table_mode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: a Join table's join has no post-build phase");
    chassert(!build_phase_finished);

    bool all_values_unique = true;
    if (single_fill_thread)
    {
        /// The rows went in during the fill; only the scratch finish and the publication remain.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::HashJoinPartitionedBuildInsertMicroseconds);
        for (auto & clause : clauses)
            all_values_unique &= clause.finishSinglePartitionInsert();
    }
    else if (clauses.size() == 1 || max_bytes_before_external_join != 0)
    {
        /// One clause after another. A clause's scatter releases only its own inputs, so the next clause
        /// still finds its keys and routes in the fill blocks. A memory budget keeps this order. The gate
        /// counts one scatter transient at a time, and each build's budget counts the tables built before
        /// it and the ones predicted after it.
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
            all_values_unique &= clauses[clause_idx].postBuild(rows, postBuildPool(clause_idx));
        }
    }
    else
    {
        /// Without a budget the clauses build at once, each on a pool of its own. The barrier waves
        /// (histogram, allocate, scatter, owner, drain) are fixed costs that add up per clause on a small
        /// build. A clause reads and releases only its own inputs of the fill blocks and writes only its
        /// own table, arenas and counters. The join's byte count is atomic. The pools are created on this
        /// thread, before any worker could create one.
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        std::vector<UInt8> clause_unique(clauses.size(), 1);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
            postBuildPool(clause_idx);
        }
        auto clause_pool = HashJoinClause::makePostBuildPool(clauses.size() - 1);
        try
        {
            for (size_t clause_idx = 1; clause_idx < clauses.size(); ++clause_idx)
                clause_pool->scheduleOrThrow(
                    [this, clause_idx, rows, &clause_unique, thread_group = CurrentThread::getGroup()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                        clause_unique[clause_idx] = clauses[clause_idx].postBuild(rows, *post_build_pools[clause_idx]);
                    });
            clause_unique[0] = clauses.front().postBuild(rows, *post_build_pools[0]);
            clause_pool->wait();
        }
        catch (...)
        {
            clause_pool->wait();
            throw;
        }
        for (UInt8 unique : clause_unique)
            all_values_unique &= unique != 0;
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    /// The routes and prepared key columns were already dropped as the scatter consumed them; this
    /// is the block shells and the lane bookkeeping, freed before the probe starts.
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    /// From here the byte count tracks only the stored blocks.
    accumulated_bytes.store(hash_join->data->allocated_size, std::memory_order_relaxed);

    UInt64 distinct_keys = 0;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        const BuildStats built = clauses[clause_idx].buildStats();
        const size_t duplicate_run_bytes = built.owner_duplicates.arena_bytes + built.drain_duplicates.arena_bytes;
        distinct_keys += built.distinct_keys;
        ProfileEvents::increment(ProfileEvents::HashJoinTableBytes, built.ht_total_bytes);
        ProfileEvents::increment(ProfileEvents::HashJoinPartitionOverflowRows, built.overflow_rows);
        ProfileEvents::increment(ProfileEvents::HashJoinDuplicateRunBytes, duplicate_run_bytes);
        LOG_TRACE(
            log,
            "Built the hash table of clause {}: {} cells in {} partitions, {} keys from {} rows ({} committed, {} overflow rows "
            "drained, {} bytes of duplicate runs)",
            clause_idx,
            built.table_cells,
            built.partitions,
            built.distinct_keys,
            built.inserted_rows,
            ReadableSize(built.ht_total_bytes),
            built.overflow_rows,
            ReadableSize(duplicate_run_bytes));
        clauses[clause_idx].releaseBuildScratch();
    }
    for (auto & pool : post_build_pools)
        pool.reset();

    /// The entry is for the next run of this query. Join reordering, `rhs_size_estimation`,
    /// runtime-filter sizing and this join's own table size (`readDistinctKeysFromStatisticsCache`)
    /// read `HashJoinEntry` whatever algorithm produced it. `ht_size` is the exact distinct count,
    /// summed over the clauses, as `HashJoin` publishes its `keys_to_join`. `hash_join` holds no stats
    /// params, so nothing else writes this key for this join.
    if (stats_collecting_params.isCollectionAndUseEnabled() && distinct_keys)
        getHashTablesStatistics<HashJoinEntry>().update(
            {.ht_size = distinct_keys, .source_rows = hash_join->data->rows_to_join}, stats_collecting_params);

    /// `HashJoin` converts and publishes from one map only; several clauses keep their hash tables.
    const bool one_clause = clauses.size() == 1;
    if (one_clause)
        clauses.front().tryConvertToFixedHashMap();
    finishBuildPhase(all_values_unique);

    /// With the table settled and its key count published: the exact runtime filter of a fixed table
    /// (8- or 16-bit keys, or a range map the conversion built) replaces the planner's Bloom filter.
    /// Only from a table that holds the whole build side; a bucket's would drop the probe rows of every
    /// other bucket.
    if (one_clause && !partial_build)
        std::visit(
            [&](const auto & shape_maps)
            {
                publishSharedFixedHashTableFilters(
                    *table_join,
                    hash_join->right_table_keys,
                    hash_join->data->type,
                    hash_join->data->key_range,
                    hash_join->data->keys_to_join.load(std::memory_order_relaxed),
                    shape_maps);
            },
            clauses.front().tableMaps().maps);

    LOG_TRACE(
        log,
        "Built {} hash table(s) with {} keys, {} of right-table data including the tables",
        clauses.size(),
        getTotalRowCount(),
        ReadableSize(getTotalByteCount()));
}

void PartitionedHashJoin::finishBuildPhase(bool all_values_unique)
{
    /// Leaf barrier over the empty map. ALL becomes RightAny when every key was unique. The probe
    /// dispatches on the promoted strictness. Flags are then resized to the whole table.
    hash_join->all_values_unique = all_values_unique;
    hash_join->onBuildPhaseFinish();
    reinitUsedFlags();
    /// Every stored block has its final number: the right-side flags of the statistics are sized
    /// per block, as `HashJoin::onBuildPhaseFinish` sizes its own.
    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(storedBlocks());
    build_phase_finished = true;
    /// Read after the flag, so the count is the table's. Before it, `getTotalRowCount` gives the fill
    /// sketches' estimate, and the barrier has already discarded the lanes with the sketches.
    hash_join->data->keys_to_join = getTotalRowCount();

    /// The fill checked the sketch's estimate of the distinct keys; this is the exact count.
    if (limits_requested.load(std::memory_order_relaxed))
        table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

ThreadPool & PartitionedHashJoin::postBuildPool(size_t clause_idx)
{
    auto & pool = post_build_pools[clause_idx];
    if (!pool)
        pool = HashJoinClause::makePostBuildPool(std::max<size_t>(1, std::min(num_threads, build_blocks.size())));
    return *pool;
}

void PartitionedHashJoin::reinitUsedFlags()
{
    /// The per-row shape marks the flags of the stored rows and never reads a per-offset flag. The
    /// `cells + 1` space would be allocated and zeroed for nothing. `HashJoin::reinitUsedFlags` skips it
    /// for the same shape.
    if (used_flags_per_row)
        return;

    /// One per-offset space of `cells + 1` (offset 0 is the zero-value cell). Must run after the leaf
    /// barrier, which sized flags to its empty map. `reinit` only grows.
    const size_t flags = clauses.front().tableCells() + 1;
    joinDispatch(
        hash_join->getKind(),
        hash_join->getStrictness(),
        hash_join->data->maps.front(),
        hash_join->getMapsKind(),
        [&](auto kind_, auto strictness_, auto & map_)
        {
            hash_join->used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map_)>()>(flags);
        });
}

JoinResultPtr PartitionedHashJoin::joinBlock(Block block)
{
    return joinBlock(std::move(block), invalid_lane);
}

JoinResultPtr PartitionedHashJoin::joinBlock(Block block, size_t lane)
{
    JoinResultPtr result;
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedProbeMicroseconds);
        result = probeDispatch(std::move(block), lane);
    }
    return std::make_unique<TimedJoinResult>(std::move(result), ProfileEvents::HashJoinPartitionedProbeMicroseconds);
}

size_t PartitionedHashJoin::getTotalRowCount() const
{
    /// The distinct keys summed over the clauses' tables, as `HashJoin` reports them: `max_rows_in_join`
    /// and the `JoinSwitcher` limit count hash table rows, not input rows. A Join table's keys are shared
    /// with the per-query instances, so they are read from the table rather than from this instance's
    /// fill counter. Before the barrier the lanes only have the sketches, so the count is their estimate,
    /// never above the rows seen.
    if (join_table_mode || build_phase_finished)
    {
        size_t keys = 0;
        for (const auto & clause : clauses)
            keys += clause.tableRowCount();
        return keys;
    }

    /// A single fill thread inserts as it goes, but the table publishes its size only when the build
    /// finishes. Until then the claimed cells are the count. They are exact between blocks, which is
    /// when the spilling wrapper's row limit and `GraceHashJoin::hasMemoryOverflow` read it.
    if (single_fill_thread && clauses.front().hasTable())
    {
        size_t keys = 0;
        for (const auto & clause : clauses)
            keys += clause.claimedTotal();
        return keys;
    }

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    if (rows == 0)
        return 0;
    size_t keys = 0;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        keys += std::min(rows, liveDistinctEstimate(clause_idx));
    return keys;
}

size_t PartitionedHashJoin::getTotalByteCount() const
{
    if (join_table_mode)
    {
        /// The storage's stored blocks (shared with the per-query instances), the table and its arena.
        const auto & data = storedData();
        return data.allocated_size + data.nullmaps_allocated_size + tablesAndArenasBytes();
    }

    return accumulated_bytes.load(std::memory_order_relaxed) + storedData().nullmaps_allocated_size + tablesAndArenasBytes();
}

size_t PartitionedHashJoin::tablesAndArenasBytes() const
{
    size_t bytes = 0;
    for (const auto & clause : clauses)
        bytes += clause.tableAndArenaBytes();
    return bytes;
}

size_t PartitionedHashJoin::bytesReservedForOtherClauses(size_t clause_idx) const
{
    /// A built table counts what it holds; one still to build counts its prediction. A table created but
    /// not yet committed holds little, so the larger of the two stands for it. The routes of every clause
    /// are in the join's byte count already.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t bytes = 0;
    for (size_t other = 0; other < clauses.size(); ++other)
    {
        if (other == clause_idx)
            continue;
        const auto & clause = clauses[other];
        bytes += std::max(
            clause.tableAndArenaBytes(), clause.predictedTableAndArenaBytes(rows, clause.distinctEstimate(), /*grouped=*/false));
    }
    return bytes;
}

size_t PartitionedHashJoin::liveDistinctEstimate(size_t clause_idx) const
{
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t last_rows = distinct_estimate_at_rows.load(std::memory_order_acquire);
    const size_t cached = cached_distinct_estimates[clause_idx].load(std::memory_order_relaxed);

    if (cached != 0 && rows <= last_rows + last_rows / 16)
        return cached;

    std::lock_guard lock(fill_mutex);
    const size_t last_rows_locked = distinct_estimate_at_rows.load(std::memory_order_relaxed);
    const size_t cached_locked = cached_distinct_estimates[clause_idx].load(std::memory_order_relaxed);
    if (cached_locked != 0 && rows <= last_rows_locked + last_rows_locked / 16)
        return cached_locked;

    /// Every clause's sketches are merged in one refresh, lane by lane: a lane in the middle of a hash
    /// pass is waited for, the others keep filling.
    const bool incremental_merge = max_bytes_before_external_join && live_estimate_gate_enabled_for_tests;
    std::vector<DenseHyperLogLog> merged(incremental_merge ? 0 : clauses.size());
    bool changed = false;
    for (const auto & lane : lanes)
    {
        std::lock_guard hll_lock(lane.hll_mutex);
        for (size_t other = 0; other < clauses.size(); ++other)
        {
            auto & sketch = lane.hll[other];
            if (incremental_merge)
            {
                if (sketch.dirty)
                {
                    live_merged_hll[other].merge(sketch);
                    sketch.dirty = false;
                    changed = true;
                }
            }
            else
            {
                merged[other].merge(sketch);
                if (max_bytes_before_external_join)
                    sketch.dirty = false;
            }
        }
    }

    if (max_bytes_before_external_join && !incremental_merge)
        for (size_t other = 0; other < clauses.size(); ++other)
            live_merged_hll[other].merge(merged[other]);

    if (incremental_merge && cached_locked != 0 && !changed)
    {
        distinct_estimate_at_rows.store(rows, std::memory_order_release);
        return cached_locked;
    }
    /// Floor at 1 so a still-empty sketch does not size the prediction as a zero-byte table. The
    /// post-build gate uses the same floor on `hll_estimate`. The value is not kept monotone: an
    /// early small-sample HyperLogLog can overshoot, and locking that in would charge duplicate-run
    /// bytes for keys that do not exist.
    size_t result = 0;
    for (size_t other = 0; other < clauses.size(); ++other)
    {
        const double distinct = incremental_merge ? live_merged_hll[other].estimate() : merged[other].estimate();
        const size_t estimate = std::max(static_cast<size_t>(std::llround(distinct)), 1uz);
        cached_distinct_estimates[other].store(estimate, std::memory_order_relaxed);
        if (other == clause_idx)
            result = estimate;
    }
    distinct_estimate_at_rows.store(rows, std::memory_order_release);
    return result;
}

bool PartitionedHashJoin::readDistinctKeysFromStatisticsCache()
{
    /// The entry `runPostBuildPhase` publishes: the exact distinct count of the previous run of this
    /// query. The cache keeps it until a run finds less than half of it, so it overstates by at most 2x.
    /// It understates only when the data grew, and then the table grows during the build, as it did
    /// before. Sized from it the table is the preallocation `HashJoin` made, counted in the same event.
    const auto hint = getSizeHint(stats_collecting_params);
    if (!hint || hint->ht_size > stats_collecting_params.max_size_to_preallocate)
        return false;
    cached_distinct_keys = hint->ht_size;
    ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, hint->ht_size);
    return true;
}

size_t PartitionedHashJoin::predictedResidentBytes(bool at_barrier) const
{
    if (single_fill_thread)
    {
        const HashJoin::Type type = hash_join->data->type;

        /// At the barrier every row is in and the resident set is what it is; only a table past its
        /// maximum fill still has a doubling ahead of it, in place with both buffers alive.
        if (at_barrier)
        {
            size_t doublings = 0;
            for (const auto & clause : clauses)
                if (clause.hasTable() && clause.claimedTotal() > clause.tableMaps().maxFill(type))
                    doublings += 2 * clause.tableMaps().getBufferSizeInBytes(type);
            return getTotalByteCount() + doublings;
        }

        /// Before the barrier the prediction follows the rows, as the partitioned branch does: the stored
        /// bytes plus the table and arenas `predictedTableAndArenaBytes` sizes for the keys seen so far.
        /// The keys are the exact claimed count, or the cached count the table was sized for when that
        /// is larger. The table this thread holds is not the measure: it starts at the smallest degree
        /// and would charge three buffers to a handful of rows. The row hint takes no part either, since
        /// it would charge a duplicate-heavy build the table of its row count. A table whose claimed keys
        /// come within one block of its maximum fill doubles on the next block, so the doubled table is
        /// the prediction then.
        const auto & data = storedData();
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        const size_t blocks = storedBlocks().size();
        const size_t rows_per_block = blocks == 0 ? rows : rows / blocks;
        size_t predicted = data.allocated_size + data.nullmaps_allocated_size;
        for (const auto & clause : clauses)
        {
            const size_t claimed = clause.hasTable() ? clause.claimedTotal() : 0;
            const size_t keys = std::max(claimed, cached_distinct_keys.value_or(0));
            size_t table = clause.predictedTableAndArenaBytes(std::max(rows, keys), keys, /*grouped=*/false);
            if (clause.hasTable() && claimed + rows_per_block > clause.tableMaps().maxFill(type))
                table = std::max(table, 2 * clause.tableMaps().getBufferSizeInBytes(type));
            predicted += table;
        }
        return predicted;
    }

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t predicted = accumulated_bytes.load(std::memory_order_relaxed);
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        predicted += clauses[clause_idx].predictedTableAndArenaBytes(rows, liveDistinctEstimate(clause_idx), /*grouped=*/false);
    return predicted;
}

size_t PartitionedHashJoin::graceInMemoryEstimateBytes() const
{
    const auto & data = storedData();
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t bytes = data.allocated_size + data.nullmaps_allocated_size;
    for (const auto & clause : clauses)
        bytes += clause.predictedTableAndArenaBytes(rows, clause.distinctEstimate(), /*grouped=*/false);
    return bytes;
}

StepAnalysisReport PartitionedHashJoin::getAnalysisReport() const
{
    /// A Join table's rows are the storage's, shared with every per-query instance.
    const size_t right_rows = join_table_mode ? storedData().rows_to_join.load() : accumulated_rows.load(std::memory_order_relaxed);

    StepAnalysisReport report;
    if (matched_rows_stats)
    {
        report = buildMatchedRowsReport(
            {.left_rows = matched_rows_stats->getInputLeft(),
             .matched_left = matched_rows_stats->getMatchedLeft(),
             .right_rows = right_rows,
             .matched_right = matched_rows_stats->getMatchedRight(right_rows)});
    }
    else
    {
        MetricList right_metrics;
        right_metrics.emplace_back(MetricKey::Rows, right_rows);
        report.push_back({MetricGroupKey::Right, std::move(right_metrics)});
    }

    MetricList hash_table_metrics;
    hash_table_metrics.emplace_back(MetricKey::UniqueKeys, getTotalRowCount());
    hash_table_metrics.emplace_back(MetricKey::Memory, getTotalByteCount());
    report.push_back({MetricGroupKey::HashTable, std::move(hash_table_metrics)});

    return report;
}

bool PartitionedHashJoin::alwaysReturnsEmptySet() const
{
    /// A Join table's rows are the storage's, shared with every per-query instance.
    const size_t rows = join_table_mode ? storedData().rows_to_join.load() : accumulated_rows.load(std::memory_order_relaxed);
    const bool empty_for_empty_right
        = isInnerOrRight(table_join->kind()) || (isLeft(table_join->kind()) && table_join->strictness() == JoinStrictness::Semi);
    return empty_for_empty_right && rows == 0;
}

PartitionedHashJoin::BuildStats PartitionedHashJoin::getBuildStats(size_t clause_idx) const
{
    BuildStats res = clauses[clause_idx].buildStats();
    res.row_store_blocks = row_store_blocks;
    return res;
}

double PartitionedHashJoin::getFillSketchEstimateForTests(size_t clause_idx)
{
    std::lock_guard lock(fill_mutex);
    DenseHyperLogLog merged;
    for (const auto & lane : lanes)
    {
        std::lock_guard hll_lock(lane.hll_mutex);
        merged.merge(lane.hll[clause_idx]);
    }
    return merged.estimate();
}

std::unique_ptr<PartitionedHashJoin::ProbeScratch> PartitionedHashJoin::acquireProbeScratch(size_t lane)
{
    if (lane < probe_scratch_slots.size())
        if (ProbeScratch * parked = probe_scratch_slots[lane].exchange(nullptr, std::memory_order_acquire))
            return std::unique_ptr<ProbeScratch>(parked);

    {
        std::lock_guard lock(probe_scratch_mutex);
        if (!probe_scratch_pool.empty())
        {
            auto scratch = std::move(probe_scratch_pool.back());
            probe_scratch_pool.pop_back();
            return scratch;
        }
    }
    return std::make_unique<ProbeScratch>();
}

void PartitionedHashJoin::releaseProbeScratch(std::unique_ptr<ProbeScratch> scratch, size_t lane)
{
    /// Park it back when the slot is free. A collision or an out-of-range lane falls through to the
    /// pool. The scratch is then neither lost nor doubly owned.
    if (lane < probe_scratch_slots.size())
    {
        ProbeScratch * expected = nullptr;
        if (probe_scratch_slots[lane].compare_exchange_strong(expected, scratch.get(), std::memory_order_release))
        {
            scratch.release(); /// NOLINT(bugprone-unused-return-value): ownership moved into the slot
            return;
        }
    }

    std::lock_guard lock(probe_scratch_mutex);
    probe_scratch_pool.push_back(std::move(scratch));
}

bool PartitionedHashJoin::isCloneSupported() const
{
    return getTotals().empty() && getTotalRowCount() == 0;
}

std::shared_ptr<IJoin>
PartitionedHashJoin::clone(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    return cloneWithBuildRowsHint(table_join_, std::move(right_sample_block_), build_rows_hint);
}

std::shared_ptr<IJoin> PartitionedHashJoin::cloneWithBuildRowsHint(
    const std::shared_ptr<TableJoin> & table_join_, SharedHeader right_sample_block_, std::optional<size_t> build_rows_hint_) const
{
    /// Every reachable clone path preserves a supported shape; re-checked so that a future caller
    /// which does not surfaces as an exception instead of wrong results.
    if (!isSupported(*table_join_))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: attempt to clone with a join shape the algorithm does not support");
    return std::make_shared<PartitionedHashJoin>(
        table_join_,
        right_sample_block_,
        num_threads,
        any_take_last_row,
        HashJoinStatsCollectingParams{.build = stats_collecting_params, .match = match_stats_collecting_params},
        max_bytes_before_external_join,
        build_rows_hint_);
}

std::shared_ptr<IJoin>
PartitionedHashJoin::cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    if (!isSupported(*table_join_))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: attempt to clone with a join shape the algorithm does not support");
    auto shard_join = std::make_shared<PartitionedHashJoin>(
        table_join_, right_sample_block_, /*num_threads_=*/1, any_take_last_row, HashJoinStatsCollectingParams{}, /*max_bytes_before_external_join_=*/0, build_rows_hint);
    shard_join->parallel_non_joined_allowed = false;
    shard_join->partial_build = true;
    return shard_join;
}

void PartitionedHashJoin::setEnableLazyColumnsIndexing(bool value)
{
    hash_join->setEnableLazyColumnsIndexing(value);
}

size_t PartitionedHashJoin::getNumFillLanes() const
{
    return lanes.size();
}

void PartitionedHashJoin::dropFillAuxiliary()
{
    for (auto & lane : lanes)
        for (auto & fill : lane.blocks)
            accumulated_bytes.fetch_sub(fill.releaseInputs(), std::memory_order_relaxed);
    for (auto & fill : build_blocks)
        accumulated_bytes.fetch_sub(fill.releaseInputs(), std::memory_order_relaxed);
}

Block PartitionedHashJoin::releaseNextFillLaneBlock(size_t lane)
{
    chassert(lane < lanes.size());
    auto & blocks = lanes[lane].blocks;
    if (blocks.empty())
        return {};

    FillBlock fill = std::move(blocks.back());
    blocks.pop_back();
    if (blocks.empty())
        blocks.shrink_to_fit();

    /// `dropFillAuxiliary` has usually released the routes already. `routeBytes` counts only what is
    /// really held.
    accumulated_bytes.fetch_sub(fill.stored.allocatedBytes() + fill.routeBytes(), std::memory_order_relaxed);
    return storedBlockToBlock(std::move(fill.stored));
}

Block PartitionedHashJoin::storedBlockToBlock(StoredBlock && stored) const
{
    const auto & data = *hash_join->data;
    return data.sample_block.cloneWithColumns(HashJoin::materializeStoredBlock(stored, data.column_access_indexes));
}

void PartitionedHashJoin::beginStoredBlockDrain()
{
    stored_blocks_released = true;
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    for (auto & pool : post_build_pools)
        pool.reset();
    for (auto & clause : clauses)
        clause.releaseTable();
}

Block PartitionedHashJoin::releaseNextStoredBlock()
{
    if (!hash_join->data)
        return {};
    if (storedBlocks().empty())
    {
        hash_join->data.reset();
        return {};
    }

    auto & data = *hash_join->data;
    auto & blocks = storedBlocks();
    StoredBlock stored = std::move(blocks.front());
    blocks.pop_front();
    data.subBytes(data.allocated_size, stored.allocatedBytes());

    Block out = storedBlockToBlock(std::move(stored));

    if (blocks.empty())
        hash_join->data.reset();
    return out;
}

BlocksList PartitionedHashJoin::releaseJoinedBlocks(bool restructure)
{
    if (build_phase_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: the right blocks were asked for after the build phase finished");

    dropFillAuxiliary();
    BlocksList blocks;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
        for (Block block = releaseNextFillLaneBlock(lane); !block.empty(); block = releaseNextFillLaneBlock(lane))
            blocks.push_back(std::move(block));
    /// A single fill thread stores as it goes and keeps no lanes.
    beginStoredBlockDrain();
    for (Block block = releaseNextStoredBlock(); !block.empty(); block = releaseNextStoredBlock())
        blocks.push_back(std::move(block));

    /// Restoring the blocks allocates, so it can throw here with the join's data already gone.
    fiu_do_on(FailPoints::hash_join_throw_after_data_release, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the join data was released");
    });

    if (restructure)
        for (auto & block : blocks)
            block = HashJoin::restoreRightBlock(block, *right_sample_block);
    return blocks;
}

const Block & PartitionedHashJoin::savedBlockSample() const
{
    return hash_join->savedBlockSample();
}

size_t PartitionedHashJoin::getRightTableRowCount() const
{
    return accumulated_rows.load(std::memory_order_relaxed);
}

void PartitionedHashJoin::drainStoredBlocksInto(IJoin & target)
{
    chassert(stored_blocks_released);

    const size_t blocks = hash_join->data ? storedBlocks().size() : 0;
    const size_t workers = std::min(num_threads, blocks);
    if (workers <= 1)
    {
        for (Block block = releaseNextStoredBlock(); !block.empty(); block = releaseNextStoredBlock())
            target.addBlockToJoin(block, block.rows(), /*worker_id=*/0, /*check_limits=*/false);
        return;
    }

    /// The pop is a list front and a few counters, so one mutex serializes it cheaply; the scatter,
    /// compression and write inside `target.addBlockToJoin` run in parallel, one target worker each.
    std::mutex pop_mutex;
    auto drain = [&](size_t worker_id)
    {
        while (true)
        {
            Block block;
            {
                std::lock_guard lock(pop_mutex);
                block = releaseNextStoredBlock();
            }
            if (block.empty())
                return;
            target.addBlockToJoin(block, block.rows(), worker_id, /*check_limits=*/false);
        }
    };

    auto pool = HashJoinClause::makePostBuildPool(workers);
    try
    {
        for (size_t w = 0; w < workers; ++w)
            pool->scheduleOrThrow(
                [&drain, w, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                    drain(w);
                });
        pool->wait();
    }
    catch (...)
    {
        pool->wait();
        throw;
    }
}

}
