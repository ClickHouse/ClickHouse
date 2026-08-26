#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/TableJoin.h>
#include <base/getL1CacheSize.h>
#include <base/getL2CacheSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <shared_mutex>

namespace ProfileEvents
{
extern const Event PartitionedHashJoinBuildMicroseconds;
extern const Event PartitionedHashJoinBuildFillMicroseconds;
extern const Event PartitionedHashJoinProbeMicroseconds;
extern const Event PartitionedHashJoinPartitions;
extern const Event PartitionedHashJoinLeafRows;
extern const Event PartitionedHashJoinTeardownMicroseconds;
extern const Event PartitionedHashJoinDistinctEstimateReused;
}

namespace CurrentMetrics
{
extern const Metric PartitionedHashJoinPoolThreads;
extern const Metric PartitionedHashJoinPoolThreadsActive;
extern const Metric PartitionedHashJoinPoolThreadsScheduled;
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
    const StatsCollectingParams & stats_collecting_params_,
    size_t max_bytes_before_external_join_)
    : table_join(std::move(table_join_))
    , right_sample_block(std::move(right_sample_block_))
    , any_take_last_row(any_take_last_row_)
    , num_threads(std::max<size_t>(1, num_threads_))
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , leaf_join(std::make_unique<HashJoin>(table_join, right_sample_block, any_take_last_row))
    , delegate_mode(!table_join->oneDisjunct())
    , maps_variant_index(leaf_join->data->maps.empty() ? 1 : leaf_join->data->maps.front().index())
    , max_fanout_per_pass(ColumnsScatter::MAX_FANOUT_PER_PASS)
    , stats_collecting_params(stats_collecting_params_)
    , log(getLogger("PartitionedHashJoin"))
{
    if (!PartitionedJoinMaps::isSupportedType(leaf_join->data->type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin was created for an unsupported map type {}; the plan-time gate must reject this shape",
            leaf_join->data->type);

    /// Sized once and never resized, because the lock-free paths index them without synchronizing
    /// against growth. A lane past the table takes the fallback.
    fill_lane_slots = std::vector<std::atomic<FillLane *>>(2 * num_threads);
    probe_scratch_slots = std::vector<std::atomic<ProbeScratch *>>(2 * num_threads);

    /// A previous run's per-partition counts replace the sketch estimate wholesale. Decided once
    /// here because every lane must fill the same way. A stale entry only mis-sizes the leaf
    /// reserves - an under-reserve grows the map, and that is counted - and the post-build always
    /// republishes fresh exact counts.
    if (!delegate_mode && stats_collecting_params.isCollectionAndUseEnabled())
        cached_stats = getHashTablesStatistics<PartitionedHashJoinEntry>().getSizeHint(stats_collecting_params);
}

bool PartitionedHashJoin::isSupported(const TableJoin & table_join)
{
    /// Everything the single-level `HashJoin` machinery serves: INNER/LEFT/RIGHT/FULL crossed with
    /// ALL/ANY/RightAny/SEMI/ANTI plus ASOF, null maps, per-clause ON filters, USING, and any number
    /// of disjuncts. What stays out: special storages, the Cross/Comma/Paste and ON-constant shapes
    /// (routed before the algorithm loop), and mixed non-equi ON conditions - `parallel_hash` serves
    /// the last better than a delegated single-threaded build would. Spilling is handled by wrapping
    /// this join in `SpillingHashJoin`, not by rejecting the shape here.
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

PartitionedHashJoin::FillLane & PartitionedHashJoin::getFillLane(size_t build_lane)
{
    if (build_lane >= fill_lane_slots.size())
        return getFillLane();

    if (FillLane * fast = fill_lane_slots[build_lane].load(std::memory_order_acquire))
        return *fast;

    /// First block of this lane: one mutexed emplace into the deque, whose elements are stable, and
    /// every later block takes the atomic load above. A lane index is unique per filling transform
    /// and a transform's work is serialized, so the slot is single-writer once published - even
    /// though executor threads migrate between transforms.
    std::lock_guard lock(fill_mutex);
    if (FillLane * raced = fill_lane_slots[build_lane].load(std::memory_order_relaxed))
        return *raced;
    FillLane * fresh = &lanes.emplace_back();
    fill_lane_slots[build_lane].store(fresh, std::memory_order_release);
    return *fresh;
}

bool PartitionedHashJoin::addBlockToJoin(const Block & source_block, bool check_limits)
{
    return addBlockToJoinImpl(source_block, check_limits, invalid_lane);
}

bool PartitionedHashJoin::addBlockToJoin(const Block & source_block, size_t /*num_rows*/, bool check_limits, size_t build_lane)
{
    /// `num_rows` only matters for the columnless CROSS blocks this algorithm never plans.
    return addBlockToJoinImpl(source_block, check_limits, build_lane);
}

bool PartitionedHashJoin::addBlockToJoinImpl(const Block & source_block, bool check_limits, size_t build_lane)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);

    if (build_phase_finished || stored_blocks_released)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: addBlockToJoin called after the build phase finished");

    if (delegate_mode)
    {
        /// The standard machinery runs the join whole, on one fill stream.
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinLeafRows, source_block.rows());
        return leaf_join->addBlockToJoin(source_block, check_limits);
    }

    /// Key preparation plus the per-row route word and sketch update. The partitioned/single-leaf
    /// decision comes later, at the barrier, so every plan pays exactly this much here.
    ProfileEventTimeIncrement<Microseconds> fill_watch(ProfileEvents::PartitionedHashJoinBuildFillMicroseconds);

    Block materialized = leaf_join->materializeColumnsFromRightBlock(source_block);
    const size_t rows = materialized.rows();
    if (rows == 0)
        return true;

    /// `RowRef::row_no` is 32-bit, as in `HashJoin`.
    if (rows > std::numeric_limits<UInt32>::max()) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for PartitionedHashJoin: {}", rows);

    FillBlock fill;
    fill.rows = rows;

    /// Exactly what the probe side does in `JoinOnKeyColumns`: materialize, keep a live
    /// LowCardinality column only for the dictionary-aware map types, extract the merged null map,
    /// strip to the nested columns. For ASOF the null map covers the inequality column too, so a
    /// row with a NULL ASOF key never joins.
    const auto & clause = table_join->getOnlyClause();
    fill.keys_holder = HashJoin::isLowCardinalityType(leaf_join->data->type)
        ? JoinCommon::materializeColumnsKeepLowCardinality(materialized, clause.key_names_right)
        : JoinCommon::materializeColumns(materialized, clause.key_names_right);
    fill.key_columns = JoinCommon::getRawPointers(fill.keys_holder);
    fill.null_map_holder = extractNestedColumnsAndNullMap(fill.key_columns, fill.null_map);

    /// Rows the ON condition filters are not inserted, but are still saved for RIGHT/FULL
    /// non-joined output.
    fill.join_mask = JoinCommon::getColumnAsMask(materialized, clause.condColumnNames().second);
    if (fill.join_mask.hasData() && fill.join_mask.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
    {
        fill.skip_bytes.resize_exact(rows);
        const NullMap * nulls = fill.null_map;
        for (size_t i = 0; i < rows; ++i)
            fill.skip_bytes[i] = ((nulls && (*nulls)[i]) || fill.join_mask.isRowFiltered(i)) ? 1 : 0;
    }

    /// One route word per row, its top 16 bits saved and the full word fed to the sketch, fused so
    /// no 32-bit column is materialized. Skipped rows are never inserted and so do not reach the
    /// sketch, but their routes are still written - the scatter's bucket derivation reads them.
    /// ASOF routes on the equi-key prefix only; its inequality column is not part of the map key.
    fill.routes.resize_exact(rows);
    FillLane & lane = build_lane == invalid_lane ? getFillLane() : getFillLane(build_lane);
    if (leaf_join->getStrictness() == JoinStrictness::Asof)
    {
        ColumnRawPtrs equi_columns(fill.key_columns.begin(), fill.key_columns.end() - 1);
        if (cached_stats)
            computeJoinRoutesForFill(equi_columns, rows, fill.routes.data());
        else
        {
            /// Exclusive merge of the sketches must not race `add` on a live lane: a torn register
            /// would persist into the barrier's `hll_estimate`, which the post-build gate then uses.
            std::shared_lock hll_lock(fill_mutex);
            computeJoinRoutesForFill(equi_columns, rows, fill.skipData(), fill.routes.data(), lane.hll);
        }
    }
    else if (cached_stats)
    {
        /// A previous run published the counts, so there is no estimate to compute.
        computeJoinRoutesForFill(fill.key_columns, rows, fill.routes.data());
    }
    else
    {
        std::shared_lock hll_lock(fill_mutex);
        computeJoinRoutesForFill(fill.key_columns, rows, fill.skipData(), fill.routes.data(), lane.hll);
    }

    /// Row-store form, payload untouched, appended to the lane without a copy.
    fill.stored = HashJoin::prepareRightBlock(materialized, leaf_join->savedBlockSample());

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
    leaf_join->checkTypesOfKeys(block);
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

void PartitionedHashJoin::storeBlocksInRowStore()
{
    const bool right_or_full = isRightOrFull(leaf_join->getKind());
    auto & data = *leaf_join->data;
    for (auto & fill : build_blocks)
    {
        assertBlocksHaveEqualStructureAllowReplicated(data.sample_block, fill.stored, "joined block");
        auto & stored = data.columns.emplace_back(fill.stored.getColumns(), ScatteredBlock::Selector(fill.rows));
        stored.block_no = data.stored_columns_index->add(&stored);
        data.allocated_size += stored.allocatedBytes();
        data.rows_to_join += fill.rows;
        fill.block_no = stored.block_no;
        fill.stored = Block{};

        if (!right_or_full)
            continue;

        /// RIGHT/FULL output needs the rows that never made it into a map - null keys and rows the
        /// ON condition filtered - exactly as the standard build saves them.
        bool save_nullmap = false;
        if (fill.null_map)
            for (size_t i = 0; i < fill.rows && !save_nullmap; ++i)
                save_nullmap = (*fill.null_map)[i];
        if (save_nullmap)
        {
            auto & holder = data.nullmaps.emplace_back(&stored, fill.null_map_holder);
            data.nullmaps_allocated_size += holder.allocatedBytes();
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
                auto & holder = data.nullmaps.emplace_back(&stored, std::move(not_joined_map));
                data.nullmaps_allocated_size += holder.allocatedBytes();
            }
        }
    }
}

void PartitionedHashJoin::decidePartitionPlan()
{
    const HashJoin::Type type = leaf_join->data->type;

    /// ASOF stays single-leaf: its mapped values are per-key sorted vectors whose insert wants the
    /// original row order, and that sorting dominates the build, so partitioning the equi-key map
    /// would pay a scattered insert order for nothing.
    bits = 0;
    if (!PartitionedJoinMaps::isFixedSizeType(type) && leaf_join->getStrictness() != JoinStrictness::Asof)
    {
        /// The fewest bits whose worst-case per-leaf reserve - the histogram clamp can only shrink
        /// it - still fits the leaf budget through the map's own grower rounding. Evaluating at the
        /// safety-scaled reserve also hedges an estimate landing on a grower boundary, where the
        /// per-leaf spread would otherwise double half the leaves.
        const size_t l2_bytes = std::max<size_t>(getL2CacheSize(), 1 << 20);
        const auto leaf_budget_bytes = static_cast<size_t>(0.8 * static_cast<double>(l2_bytes));
        const auto reserve_for = [&](size_t fanout)
        { return std::max<size_t>(1, static_cast<size_t>(std::ceil(hll_estimate * reserve_safety / static_cast<double>(fanout)))); };

        while (bits < 16
               && PartitionedJoinMaps::predictedBufferBytes(maps_variant_index, type, reserve_for(1uz << bits)) > leaf_budget_bytes)
            ++bits;

        /// Past this many leaves the descriptor array stops fitting the cache it is gathered from
        /// once per probe row, and buying L2-resident leaf buckets with a cold descriptor gather
        /// plus a second scatter pass is a net loss. Budgeted against L1, charging the array only a
        /// quarter of it so the rest of the probe's per-row working set - the cell the descriptor
        /// points at, the key and result columns - still fits alongside.
        const size_t l1_bytes = std::max<size_t>(getL1CacheSize(), 32 << 10);
        const size_t max_leaves_for_descs = std::max<size_t>(1, l1_bytes / 4 / sizeof(LeafMapDesc));
        const auto descriptor_cap_bits = static_cast<size_t>(std::bit_width(max_leaves_for_descs) - 1);
        bits = std::min(bits, descriptor_cap_bits);

        if (bits > 0)
        {
            /// Once partitioning pays for itself, at least one leaf per worker, so the leaf builds
            /// parallelize. A small build stays single-leaf.
            const auto parallelism_floor = static_cast<size_t>(std::bit_width(std::bit_ceil(num_threads) - 1));
            bits = std::max(bits, parallelism_floor);
        }
    }

    partitions = 1uz << bits;

    /// When the budget wants a wider fanout than one scatter pass sustains, the bits split into
    /// MSB-first passes rather than the fanout being capped. The 16-bit bound above is what the
    /// saved routes and the UInt16 probe leaf ids cover, so no reachable plan needs wider routes.
    pass_bits = bits > 0 ? ColumnsScatter::computePassBits(partitions, max_fanout_per_pass) : std::vector<size_t>{};
}

void PartitionedHashJoin::onBuildPhaseFinish()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);

    if (delegate_mode)
    {
        /// The standard machinery already built during the fill; only its own barrier remains.
        leaf_join->onBuildPhaseFinish();
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinPartitions, partitions);
        return;
    }

    /// Run once by the last fill thread, and deliberately cheap: concatenate the lanes, number the
    /// row-store blocks, merge the sketches, pick the plan. The scatter, allocation and leaf builds
    /// are `runPostBuildPhase`'s work. Exclusive so this merge cannot race a still-running fill
    /// `add` the same way `liveDistinctEstimate` cannot.
    DenseHyperLogLog merged;
    size_t total_blocks = 0;
    {
        std::lock_guard lock(fill_mutex);
        for (const auto & lane : lanes)
            total_blocks += lane.blocks.size();
        build_blocks.reserve(total_blocks);
        for (auto & lane : lanes)
        {
            merged.merge(lane.hll);
            for (auto & block : lane.blocks)
                build_blocks.push_back(std::move(block));
            lane.blocks.clear();
        }
        lanes.clear();
        lane_by_thread.clear();
    }

    if (cached_stats)
    {
        /// The sketches were never fed, so the cached total drives the partition count and
        /// `sizeLeafHashTables` consumes the per-partition breakdown. Both are clamped per leaf by the
        /// exact row counts, so a stale value cannot inflate a leaf past its own rows.
        hll_estimate = static_cast<double>(std::max<size_t>(1, cached_stats->total_distinct));
        stats.distinct_estimate_reused = true;
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinDistinctEstimateReused);
    }
    else
    {
        hll_estimate = merged.estimate();
    }
    storeBlocksInRowStore();

    /// Typical pipelines deliver blocks under 65536 rows, so the packed encoding usually applies and
    /// halves the locator transient.
    narrow_locators = build_blocks.size() <= (1uz << 16);
    for (const auto & fill : build_blocks)
        narrow_locators = narrow_locators && fill.block_no < (1u << 16) && fill.rows <= (1uz << 16);

    decidePartitionPlan();
    ProfileEvents::increment(ProfileEvents::PartitionedHashJoinPartitions, partitions);

    LOG_TRACE(
        log,
        "Partition plan: bits = {}, partitions = {}, {} scatter pass(es) (bits per pass [{}]), {} rows in {} blocks, "
        "estimated {} distinct keys",
        bits,
        partitions,
        std::max<size_t>(pass_bits.size(), 1),
        fmt::join(pass_bits, ", "),
        accumulated_rows.load(std::memory_order_relaxed),
        build_blocks.size(),
        static_cast<size_t>(hll_estimate));
}

JoinResultPtr PartitionedHashJoin::joinBlock(Block block)
{
    return joinBlock(std::move(block), invalid_lane);
}

JoinResultPtr PartitionedHashJoin::joinBlock(Block block, size_t lane)
{
    JoinResultPtr result;
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinProbeMicroseconds);
        result = delegate_mode ? leaf_join->joinBlock(std::move(block)) : probeDispatch(std::move(block), lane);
    }
    return std::make_unique<TimedJoinResult>(std::move(result), ProfileEvents::PartitionedHashJoinProbeMicroseconds);
}

size_t PartitionedHashJoin::getTotalRowCount() const
{
    if (delegate_mode)
        return leaf_join->getTotalRowCount();

    if (!build_phase_finished)
        return accumulated_rows.load(std::memory_order_relaxed);

    const HashJoin::Type type = leaf_join->data->type;
    size_t res = 0;
    for (const auto & maps : leaf_maps)
        res += maps.getTotalRowCount(type);
    return res;
}

size_t PartitionedHashJoin::getTotalByteCount() const
{
    if (delegate_mode)
        return leaf_join->getTotalByteCount();

    size_t res = accumulated_bytes.load(std::memory_order_relaxed);
    const HashJoin::Type type = leaf_join->data->type;
    for (const auto & maps : leaf_maps)
        res += maps.getBufferSizeInBytes(type);
    for (const auto & arena : build_arenas)
        res += arena.allocatedBytes();
    return res;
}

size_t PartitionedHashJoin::liveDistinctEstimate() const
{
    /// A previous run published the counts, so the sketches were never fed.
    if (cached_stats)
        return std::min(accumulated_rows.load(std::memory_order_relaxed), cached_stats->total_distinct);

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
    /// early small-sample HyperLogLog can overshoot, and locking that in would charge list-arena
    /// bytes for keys that do not exist.
    const size_t estimate = std::max(static_cast<size_t>(std::llround(merged.estimate())), 1uz);
    cached_distinct_estimate.store(estimate, std::memory_order_relaxed);
    distinct_estimate_at_rows.store(rows, std::memory_order_release);
    return estimate;
}

size_t PartitionedHashJoin::predictedResidentBytes() const
{
    if (delegate_mode)
        return leaf_join->getTotalByteCount();

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t bytes = accumulated_bytes.load(std::memory_order_relaxed);
    return bytes + predictedTableAndArenaBytes(rows, liveDistinctEstimate());
}

StepAnalysisReport PartitionedHashJoin::getAnalysisReport() const
{
    if (delegate_mode)
        return leaf_join->getAnalysisReport();

    /// No per-side matched counters yet: the routed probe does not feed the `HashJoin` stats the
    /// standard path collects them in, so only the sizes the leaves themselves know are reported.
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
        return leaf_join->alwaysReturnsEmptySet();
    return isInnerOrRight(table_join->kind()) && accumulated_rows.load(std::memory_order_relaxed) == 0;
}

PartitionedHashJoin::BuildStats PartitionedHashJoin::getBuildStats() const
{
    BuildStats res = stats;
    res.bits = bits;
    res.partitions = partitions;
    res.pass_bits = pass_bits;
    res.hll_estimate = hll_estimate;
    res.ht_total_bytes = ht_total_bytes;
    res.amac_ring_growths = amac_ring_growths.load(std::memory_order_relaxed);
    res.amac_build_engaged = amac_build_engaged;
    res.flag_base = flag_base;
    return res;
}

std::unique_ptr<PartitionedHashJoin::ProbeScratch> PartitionedHashJoin::acquireProbeScratch(size_t lane)
{
    /// One atomic exchange to take this stream's parked scratch.
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
    /// Park it back when the slot is free; a collision or an out-of-range lane falls through to the
    /// pool, so the scratch is neither lost nor doubly owned.
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
        table_join_, right_sample_block_, num_threads, any_take_last_row, stats_collecting_params, max_bytes_before_external_join);
}

std::shared_ptr<IJoin>
PartitionedHashJoin::cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    return std::make_shared<HashJoin>(table_join_, right_sample_block_, any_take_last_row);
}

void PartitionedHashJoin::setEnableLazyColumnsIndexing(bool value)
{
    leaf_join->setEnableLazyColumnsIndexing(value);
}

size_t PartitionedHashJoin::getNumFillLanes() const
{
    return lanes.size();
}

void PartitionedHashJoin::dropFillAuxiliary()
{
    auto drop_one = [this](FillBlock & fill)
    {
        const size_t route_bytes = fill.routes.allocated_bytes();
        fill.keys_holder.clear();
        fill.key_columns.clear();
        fill.null_map_holder.reset();
        fill.null_map = nullptr;
        fill.join_mask = JoinCommon::JoinMask();
        fill.skip_bytes = {};
        fill.routes = {};
        if (route_bytes)
            accumulated_bytes.fetch_sub(route_bytes, std::memory_order_relaxed);
    };

    for (auto & lane : lanes)
        for (auto & fill : lane.blocks)
            drop_one(fill);
    for (auto & fill : build_blocks)
        drop_one(fill);
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

    const size_t freed = fill.stored.allocatedBytes() + fill.routes.allocated_bytes();
    accumulated_bytes.fetch_sub(freed, std::memory_order_relaxed);
    return std::move(fill.stored);
}

void PartitionedHashJoin::beginStoredBlockDrain()
{
    stored_blocks_released = true;
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    post_build_ctx.reset();
    post_build_pool.reset();
    build_arenas.clear();
}

Block PartitionedHashJoin::releaseNextStoredBlock()
{
    if (!leaf_join->data || leaf_join->data->columns.empty())
    {
        if (leaf_join->data)
            leaf_join->data.reset();
        return {};
    }

    auto & data = *leaf_join->data;
    StoredBlock stored = std::move(data.columns.front());
    data.columns.pop_front();
    const size_t stored_bytes = stored.allocatedBytes();
    if (data.allocated_size >= stored_bytes)
        data.allocated_size -= stored_bytes;
    else
        data.allocated_size = 0;

    Block block = data.sample_block.cloneWithColumns(stored.columns);
    ScatteredBlock scattered(std::move(block), std::move(stored.selector));
    scattered.filterBySelector();
    Block out = std::move(scattered.getSourceBlock());

    if (data.columns.empty())
        leaf_join->data.reset();
    return out;
}

}
