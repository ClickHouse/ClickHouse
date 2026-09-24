#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <mutex>

namespace ProfileEvents
{
extern const Event HashJoinPartitionedBuildMicroseconds;
extern const Event HashJoinPartitionedBuildFillMicroseconds;
extern const Event HashJoinPartitionedBuildInsertMicroseconds;
extern const Event HashJoinPartitionedProbeMicroseconds;
extern const Event HashJoinPartitions;
extern const Event HashJoinInsertedRows;
extern const Event HashJoinTableBytes;
extern const Event HashJoinPartitionOverflowRows;
extern const Event HashJoinDuplicateRunBytes;
extern const Event HashJoinTeardownMicroseconds;
extern const Event HashJoinRowStoreBlocks;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int SET_SIZE_LIMIT_EXCEEDED;
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
    : table_join(std::move(table_join_))
    , right_sample_block(std::move(right_sample_block_))
    , any_take_last_row(any_take_last_row_)
    , num_threads(std::max<size_t>(1, num_threads_))
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , hash_join(
          std::make_unique<HashJoin>(
              table_join,
              right_sample_block,
              any_take_last_row,
              /*reserve_num_=*/0,
              /*instance_id_=*/"",
              /*stats_collecting_params_=*/HashJoinStatsCollectingParams{},
              /*max_threads_=*/1,
              /*use_parallel_layout_=*/false,
              /*allow_set_maps_=*/false))
    , delegate_mode(!table_join->oneDisjunct())
    , build_rows_hint(build_rows_hint_)
    , single_fill_thread(!delegate_mode && build_rows_hint_ && *build_rows_hint_ < table_join->parallelHashJoinThreshold())
    , stats_collecting_params(stats_collecting_params_.build)
    , match_stats_collecting_params(stats_collecting_params_.match)
    , log(getLogger("PartitionedHashJoin"))
    , clause(*hash_join, *table_join, any_take_last_row, num_threads, max_bytes_before_external_join, build_blocks, accumulated_bytes, log)
{
    if (!HashJoinTableMaps::isSupportedType(hash_join->data->type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin was created for an unsupported map type {}; the plan-time gate must reject this shape",
            hash_join->data->type);

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

    if (!delegate_mode && !single_fill_thread && max_bytes_before_external_join == 0)
    {
        const auto hint = getSizeHint(stats_collecting_params);
        if (hint && hint->ht_size <= stats_collecting_params.max_size_to_preallocate)
            cached_distinct_keys = hint->ht_size;
    }
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

    clause.releaseTable();
    hash_join.reset();
    probe_scratch_pool.clear();
    for (auto & slot : probe_scratch_slots)
        delete slot.load(std::memory_order_acquire);
}

bool PartitionedHashJoin::isSupported(const TableJoin & table_join)
{
    /// Everything the single-level `HashJoin` machinery serves. Kinds: INNER, LEFT, RIGHT, FULL.
    /// Strictness: ALL, ANY, RightAny, SEMI, ANTI, plus ASOF. Also null maps, per-clause ON filters,
    /// USING, and any number of disjuncts. Out: special storages, and the Cross/Comma/Paste and
    /// ON-constant joins. Those are routed before the algorithm loop. Also out: mixed non-equi ON
    /// conditions. The parallel `hash` layout serves those better than a delegated single-threaded
    /// build would.
    /// A memory limit is no reason to decline: the planner wraps this join in `SpillingHashJoin` instead.
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
        case JoinStrictness::Asof: break;
        default: return false;
    }

    if (table_join.isSpecialStorage())
        return false;
    if (table_join.getMixedJoinExpression())
        return false;

    if (strictness == JoinStrictness::Asof)
    {
        /// The same restrictions `HashJoin` applies.
        if (!isInnerOrLeft(kind) || !table_join.oneDisjunct())
            return false;
        if (table_join.getOnlyClause().key_names_right.size() <= 1)
            return false;
    }

    /// The keyless clauses have their own plan-time routing.
    for (const auto & clause : table_join.getClauses())
        if (clause.key_names_right.empty())
            return false;

    return true;
}

const TableJoin & PartitionedHashJoin::getTableJoin() const
{
    return *table_join;
}

PartitionedHashJoin::FillLane & PartitionedHashJoin::getFillLane()
{
    std::lock_guard lock(fill_mutex);
    auto [it, inserted] = lane_by_thread.try_emplace(std::this_thread::get_id(), nullptr);
    if (inserted)
        it->second = &lanes.emplace_back();
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
    FillLane * fresh = &lanes.emplace_back();
    fill_lane_slots[worker_id].store(fresh, std::memory_order_release);
    return *fresh;
}

bool PartitionedHashJoin::addBlockToJoin(const Block & source_block, size_t /*num_rows*/, size_t worker_id, bool check_limits)
{
    /// `num_rows` only matters for the columnless CROSS blocks this algorithm never plans.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (build_phase_finished || stored_blocks_released)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: addBlockToJoin called after the build phase finished");

    if (delegate_mode)
    {
        /// The standard machinery runs the join whole, on one fill stream, so the inner join has one worker.
        ProfileEvents::increment(ProfileEvents::HashJoinInsertedRows, source_block.rows());
        return hash_join->addBlockToJoin(source_block, source_block.rows(), /*worker_id=*/0, check_limits);
    }

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

    /// Exactly what the probe side does in `JoinOnKeyColumns`. Materialize. Keep a live
    /// LowCardinality column only for the dictionary-aware map types. Extract the merged null map.
    /// Strip to the nested columns. For ASOF the null map covers the inequality column too, so a
    /// row with a NULL ASOF key never joins.
    const auto & on_clause = table_join->getOnlyClause();
    fill.keys_holder = HashJoin::isLowCardinalityType(hash_join->data->type)
        ? JoinCommon::materializeColumnsKeepLowCardinality(materialized, on_clause.key_names_right)
        : JoinCommon::materializeColumns(materialized, on_clause.key_names_right);
    fill.key_columns = JoinCommon::getRawPointers(fill.keys_holder);
    fill.null_map_holder = extractNestedColumnsAndNullMap(fill.key_columns, fill.null_map);

    /// Rows the ON condition filters are not inserted, but are still saved for RIGHT/FULL
    /// non-joined output.
    fill.join_mask = JoinCommon::getColumnAsMask(materialized, on_clause.condColumnNames().second);
    if (fill.join_mask.hasData() && fill.join_mask.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
    {
        fill.skip_bytes.resize_exact(rows);
        const NullMap * nulls = fill.null_map;
        for (size_t i = 0; i < rows; ++i)
            fill.skip_bytes[i] = ((nulls && (*nulls)[i]) || fill.join_mask.isRowFiltered(i)) ? 1 : 0;
    }

    /// The payload in stored form. The constructor already decided the row store layout. The columns
    /// it admits are packed row-wise here. The probe then reads one row pointer per output row,
    /// instead of one random column read per output column. The remaining columns stay columnar.
    Block prepared = HashJoin::prepareRightBlock(materialized, hash_join->savedBlockSample());
    assertBlocksHaveEqualStructureAllowReplicated(hash_join->data->sample_block, prepared, "joined block");
    fill.stored = hash_join->createStoredBlock(prepared, ScatteredBlock::Selector(rows));

    if (single_fill_thread)
    {
        /// One fill thread and no partition plan, so no routes and no sketch. The block is stored and
        /// its rows go into the table right away. The table grows like `hash`'s when the hint was low.
        /// The fill block stays alive through the insert because its key pointers point into its holders.
        accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
        accumulated_bytes.fetch_add(fill.stored.allocatedBytes(), std::memory_order_relaxed);
        storeBlockInRowStore(fill);
        /// The table is sized from the planner's estimate before the first block; a low estimate
        /// only costs doublings during the inserts.
        if (!clause.hasTable())
            clause.beginSinglePartitionInsert(
                clause.reserveFor(*build_rows_hint, static_cast<double>(*build_rows_hint)),
                accumulated_rows.load(std::memory_order_relaxed),
                /*grow_at_max_fill_=*/true);
        clause.insertSingleLaneBlock(fill);

        if (!check_limits)
            return true;
        return table_join->sizeLimits().check(
            accumulated_rows.load(std::memory_order_relaxed),
            accumulated_bytes.load(std::memory_order_relaxed),
            "JOIN",
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }

    FillLane & lane = getFillLane(worker_id);
    if (max_bytes_before_external_join)
    {
        /// Keep a live estimate on the spill path. The reader takes this lock exclusively, so it
        /// observes complete blocks at the same points as before the no-spill lock removal.
        std::shared_lock hll_lock(fill_mutex);
        clause.computeRoutes(fill, lane.hll);
    }
    else if (cached_distinct_keys)
        clause.computeRoutes(fill);
    else
        clause.computeRoutes(fill, lane.hll);

    accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
    accumulated_bytes.fetch_add(fill.stored.allocatedBytes() + fill.routes.allocated_bytes(), std::memory_order_relaxed);
    lane.blocks.push_back(std::move(fill));

    if (!check_limits)
        return true;

    /// The fill-phase analogue of `HashJoin`'s per-block limit check. Rows are the accumulated
    /// input rows, an upper bound on the keys the map-based algorithms check; bytes cover the stored
    /// blocks and the route transients.
    return table_join->sizeLimits().check(
        accumulated_rows.load(std::memory_order_relaxed),
        accumulated_bytes.load(std::memory_order_relaxed),
        "JOIN",
        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
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

void PartitionedHashJoin::storeBlockInRowStore(FillBlock & fill)
{
    auto & data = *hash_join->data;
    auto & stored = storedBlocks().emplace_back(std::move(fill.stored));
    stored.block_no = data.stored_columns_index->add(&stored);
    data.addBytes(data.allocated_size, stored.allocatedBytes());
    data.rows_to_join.fetch_add(fill.rows, std::memory_order_relaxed);
    fill.block_no = stored.block_no;
    fill.stored = StoredBlock{};
    if (stored.hasRowStore())
    {
        ++row_store_blocks;
        ProfileEvents::increment(ProfileEvents::HashJoinRowStoreBlocks);
    }

    if (!isRightOrFull(hash_join->getKind()))
        return;

    /// RIGHT/FULL output needs the rows that never made it into the table - null keys and rows the
    /// ON condition filtered - exactly as the standard build saves them.
    bool save_nullmap = false;
    if (fill.null_map)
        for (size_t i = 0; i < fill.rows && !save_nullmap; ++i)
            save_nullmap = (*fill.null_map)[i];
    if (save_nullmap)
    {
        auto & holder = storedNullmaps().emplace_back(&stored, fill.null_map_holder);
        data.addBytes(data.nullmaps_allocated_size, holder.allocatedBytes());
    }

    if (fill.join_mask.hasData() && fill.join_mask.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
    {
        auto not_joined_map = ColumnUInt8::create(fill.rows, static_cast<UInt8>(0));
        bool has_right_not_joined = false;
        for (size_t i = 0; i < fill.rows; ++i)
        {
            if (!fill.join_mask.isRowFiltered(i))
                continue;
            if (save_nullmap && (*fill.null_map)[i])
                continue; /// already covered by the null-keys map
            not_joined_map->getData()[i] = 1;
            has_right_not_joined = true;
        }
        if (has_right_not_joined)
        {
            auto & holder = storedNullmaps().emplace_back(&stored, std::move(not_joined_map));
            data.addBytes(data.nullmaps_allocated_size, holder.allocatedBytes());
        }
    }
}

void PartitionedHashJoin::onBuildPhaseFinish()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (delegate_mode)
    {
        /// The standard machinery already built during the fill; only its own barrier remains.
        hash_join->onBuildPhaseFinish();
        ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clause.partitionCount());
        return;
    }

    if (single_fill_thread)
    {
        /// Everything was inserted as it arrived. A build that never saw a block still needs its table,
        /// because the used flags and the probe are sized from it. The exact distinct count stands in
        /// for the sketch estimate the memory gate reads.
        if (!clause.hasTable())
            clause.beginSinglePartitionInsert(
                clause.reserveFor(1, 1.0), accumulated_rows.load(std::memory_order_relaxed), /*grow_at_max_fill_=*/true);
        clause.setDistinctEstimate(static_cast<double>(clause.claimedTotal()));
        ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clause.partitionCount());
        LOG_TRACE(
            log,
            "Single fill thread: table of 2^{} cells, {} rows in {} blocks inserted during the fill, {} distinct keys",
            clause.sizeDegree(),
            accumulated_rows.load(std::memory_order_relaxed),
            storedBlocks().size(),
            static_cast<size_t>(clause.hllEstimate()));
        return;
    }

    /// Run once by the last fill thread, and deliberately cheap: concatenate the lanes, number the
    /// row-store blocks, merge cold-build sketches, and pick the plan. The scatter, allocation and
    /// inserts are `runPostBuildPhase`'s work. The barrier takes the live spill reader's exclusive lock.
    DenseHyperLogLog merged;
    size_t total_blocks = 0;
    {
        std::lock_guard lock(fill_mutex);
        for (const auto & lane : lanes)
            total_blocks += lane.blocks.size();
        build_blocks.reserve(total_blocks);
        for (auto & lane : lanes)
        {
            if (!cached_distinct_keys)
                merged.merge(lane.hll);
            for (auto & block : lane.blocks)
                build_blocks.push_back(std::move(block));
            lane.blocks.clear();
        }
        lanes.clear();
        lane_by_thread.clear();
    }

    clause.setDistinctEstimate(
        cached_distinct_keys ? static_cast<double>(*cached_distinct_keys) : merged.estimate(), cached_distinct_keys.has_value());
    for (auto & fill : build_blocks)
        storeBlockInRowStore(fill);
    clause.decidePartitionPlan(accumulated_rows.load(std::memory_order_relaxed));
    ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clause.partitionCount());
}

PartitionedHashJoin::PostBuildPlan PartitionedHashJoin::planPostBuild()
{
    if (max_bytes_before_external_join == 0 || delegate_mode)
        return PostBuildPlan::Fits;

    /// Everything is already resident: the stored blocks, the table and the duplicate runs.
    if (single_fill_thread)
        return getTotalByteCount() <= max_bytes_before_external_join ? PostBuildPlan::Fits : PostBuildPlan::MustSpill;

    return clause.planPostBuild(accumulated_rows.load(std::memory_order_relaxed));
}

void PartitionedHashJoin::runPostBuildPhase()
{
    chassert(!build_phase_finished);

    if (delegate_mode)
    {
        /// Already built during the fill and the barrier. Its single-map post-build optimizations
        /// stay off, as they do on the partitioned path.
        build_phase_finished = true;
        return;
    }

    bool all_values_unique = true;
    if (single_fill_thread)
    {
        /// The rows went in during the fill; only the scratch finish and the publication remain.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::HashJoinPartitionedBuildInsertMicroseconds);
        all_values_unique = clause.finishSinglePartitionInsert();
    }
    else
        all_values_unique = clause.postBuild(accumulated_rows.load(std::memory_order_relaxed));

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    /// The routes and prepared key columns were already dropped as the scatter consumed them; this
    /// is the block shells and the lane bookkeeping, freed before the probe starts.
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    /// From here the byte count tracks only the stored blocks.
    accumulated_bytes.store(hash_join->data->allocated_size, std::memory_order_relaxed);

    const BuildStats built = clause.buildStats();
    ProfileEvents::increment(ProfileEvents::HashJoinTableBytes, built.ht_total_bytes);
    ProfileEvents::increment(ProfileEvents::HashJoinPartitionOverflowRows, built.overflow_rows);
    ProfileEvents::increment(
        ProfileEvents::HashJoinDuplicateRunBytes, built.owner_duplicates.arena_bytes + built.drain_duplicates.arena_bytes);

    /// `ht_size` is this build's exact distinct count. The next run can use it for planning and
    /// warm table sizing. `hash_join` holds no stats params, so nothing else writes this key.
    if (stats_collecting_params.isCollectionAndUseEnabled() && built.distinct_keys)
        getHashTablesStatistics<HashJoinEntry>().update(
            {.ht_size = built.distinct_keys, .source_rows = hash_join->data->rows_to_join}, stats_collecting_params);

    clause.releaseBuildScratch();

    finishBuildPhase(all_values_unique);

    LOG_TRACE(
        log,
        "Built one shared hash table of {} cells in {} partitions: {} keys from {} rows, {} of right-table data including the table "
        "({} committed, {} overflow rows drained, {} bytes of duplicate runs)",
        built.table_cells,
        built.partitions,
        built.distinct_keys,
        built.inserted_rows,
        ReadableSize(getTotalByteCount()),
        ReadableSize(built.ht_total_bytes),
        built.overflow_rows,
        ReadableSize(built.owner_duplicates.arena_bytes + built.drain_duplicates.arena_bytes));
}

void PartitionedHashJoin::finishBuildPhase(bool all_values_unique)
{
    /// Leaf barrier over the empty map. ALL becomes RightAny when every key was unique. The probe
    /// dispatches on the promoted strictness. Flags are then resized to the whole table.
    hash_join->all_values_unique = all_values_unique;
    hash_join->onBuildPhaseFinish();
    reinitUsedFlags();
    hash_join->data->keys_to_join = getTotalRowCount();
    build_phase_finished = true;
}

void PartitionedHashJoin::reinitUsedFlags()
{
    /// One per-offset space of `cells + 1` (offset 0 is the zero-value cell). Must run after the leaf
    /// barrier, which sized flags to its empty map. `reinit` only grows.
    const size_t flags = clause.tableCells() + 1;
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
        result = delegate_mode ? hash_join->joinBlock(std::move(block)) : probeDispatch(std::move(block), lane);
    }
    return std::make_unique<TimedJoinResult>(std::move(result), ProfileEvents::HashJoinPartitionedProbeMicroseconds);
}

size_t PartitionedHashJoin::getTotalRowCount() const
{
    if (delegate_mode)
        return hash_join->getTotalRowCount();

    if (!build_phase_finished || !clause.hasTable())
        return accumulated_rows.load(std::memory_order_relaxed);

    return clause.tableRowCount();
}

size_t PartitionedHashJoin::getTotalByteCount() const
{
    if (delegate_mode)
        return hash_join->getTotalByteCount();

    return accumulated_bytes.load(std::memory_order_relaxed) + storedData().nullmaps_allocated_size + clause.tableAndArenaBytes();
}

size_t PartitionedHashJoin::liveDistinctEstimate() const
{
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t last_rows = distinct_estimate_at_rows.load(std::memory_order_acquire);
    const size_t cached = cached_distinct_estimate.load(std::memory_order_relaxed);

    if (cached != 0 && rows <= last_rows + last_rows / 16)
        return cached;

    std::lock_guard lock(fill_mutex);
    const size_t last_rows_locked = distinct_estimate_at_rows.load(std::memory_order_relaxed);
    const size_t cached_locked = cached_distinct_estimate.load(std::memory_order_relaxed);
    if (cached_locked != 0 && rows <= last_rows_locked + last_rows_locked / 16)
        return cached_locked;

    DenseHyperLogLog merged;
    for (const auto & lane : lanes)
        merged.merge(lane.hll);

    /// Floor at 1 so a still-empty sketch does not size the prediction as a zero-byte table. The
    /// post-build gate uses the same floor on `hll_estimate`. The value is not kept monotone: an
    /// early small-sample HyperLogLog can overshoot, and locking that in would charge duplicate-run
    /// bytes for keys that do not exist.
    const size_t estimate = std::max(static_cast<size_t>(std::llround(merged.estimate())), 1uz);
    cached_distinct_estimate.store(estimate, std::memory_order_relaxed);
    distinct_estimate_at_rows.store(rows, std::memory_order_release);
    return estimate;
}

size_t PartitionedHashJoin::predictedResidentBytes(bool at_barrier) const
{
    if (delegate_mode)
        return hash_join->getTotalByteCount();

    if (single_fill_thread)
    {
        /// The table already exists and doubles in place while the old buffer is still alive, so the
        /// peak ahead is the resident set plus two more table buffers. At the barrier every row is in,
        /// and only a table past its maximum fill still has that doubling ahead of it.
        const HashJoin::Type type = hash_join->data->type;
        const size_t table_bytes = clause.hasTable() ? clause.tableMaps().getBufferSizeInBytes(type) : 0;
        if (at_barrier && (!clause.hasTable() || clause.claimedTotal() <= clause.tableMaps().maxFill(type)))
            return getTotalByteCount();
        return getTotalByteCount() + 2 * table_bytes;
    }

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t bytes = accumulated_bytes.load(std::memory_order_relaxed);
    return bytes + clause.predictedTableAndArenaBytes(rows, liveDistinctEstimate(), /*grouped=*/false);
}

size_t PartitionedHashJoin::graceInMemoryEstimateBytes() const
{
    const auto & data = storedData();
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    return data.allocated_size + data.nullmaps_allocated_size
        + clause.predictedTableAndArenaBytes(rows, clause.distinctEstimate(), /*grouped=*/false);
}

StepAnalysisReport PartitionedHashJoin::getAnalysisReport() const
{
    if (delegate_mode)
        return hash_join->getAnalysisReport();

    /// Only the sizes the table itself knows: the probe does not feed the per-side matched counters
    /// `HashJoin` collects.
    StepAnalysisReport report;

    MetricList right_metrics;
    right_metrics.emplace_back(MetricKey::Rows, accumulated_rows.load(std::memory_order_relaxed));
    report.push_back({MetricGroupKey::Right, std::move(right_metrics)});

    MetricList hash_table_metrics;
    hash_table_metrics.emplace_back(MetricKey::UniqueKeys, getTotalRowCount());
    hash_table_metrics.emplace_back(MetricKey::Memory, getTotalByteCount());
    report.push_back({MetricGroupKey::HashTable, std::move(hash_table_metrics)});

    return report;
}

bool PartitionedHashJoin::alwaysReturnsEmptySet() const
{
    if (delegate_mode)
        return hash_join->alwaysReturnsEmptySet();
    const bool empty_for_empty_right
        = isInnerOrRight(table_join->kind()) || (isLeft(table_join->kind()) && table_join->strictness() == JoinStrictness::Semi);
    return empty_for_empty_right && accumulated_rows.load(std::memory_order_relaxed) == 0;
}

PartitionedHashJoin::BuildStats PartitionedHashJoin::getBuildStats() const
{
    BuildStats res = clause.buildStats();
    res.row_store_blocks = row_store_blocks;
    return res;
}

double PartitionedHashJoin::getFillSketchEstimateForTests()
{
    std::lock_guard lock(fill_mutex);
    DenseHyperLogLog merged;
    for (const auto & lane : lanes)
        merged.merge(lane.hll);
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
        build_rows_hint);
}

std::shared_ptr<IJoin>
PartitionedHashJoin::cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    return std::make_shared<HashJoin>(
        table_join_,
        right_sample_block_,
        any_take_last_row,
        /*reserve_num_=*/0,
        /*instance_id_=*/"",
        HashJoinStatsCollectingParams{},
        /*max_threads_=*/1,
        /*use_parallel_layout_=*/false);
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

    /// `dropFillAuxiliary` has usually released the routes already and left an empty array. Its
    /// `allocated_bytes` still reports the padding, so count only what is really held.
    const size_t route_bytes = fill.routes.empty() ? 0 : fill.routes.allocated_bytes();
    accumulated_bytes.fetch_sub(fill.stored.allocatedBytes() + route_bytes, std::memory_order_relaxed);
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
