#include <Interpreters/HashJoin/HashJoinBuild.h>

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinPartitionedBuild.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <bit>
#include <span>

namespace ProfileEvents
{
extern const Event HashJoinPreallocatedElementsInHashTables;
extern const Event HashJoinBuildRounds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int QUERY_WAS_CANCELLED;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

/// A live estimate is refreshed once the waiting rows grew by this fraction since the last refresh.
constexpr size_t REFRESH_GROWTH_DIVISOR = 16;
/// The live test: fewer than one new key in this many rows means that keys have stopped arriving.
constexpr size_t REPEATS_ONLY_RATIO = 16;
/// The live test starts once this many rows wait: the default of `parallel_hash_join_threshold`, the size from which a join
/// splits its table. A smaller build gains little from dropping its blocks early.
constexpr size_t LIVE_TEST_MIN_ROWS = 100'000;
/// The backstop takes this fraction of the budget.
constexpr size_t BACKSTOP_DIVISOR = 4;
/// The lazy sketch of one build stream starts when the byte bound reaches this fraction of a budget or a byte limit.
constexpr size_t LAZY_SKETCH_DIVISOR = 4;
/// Without an estimate, an `ALL` row-list arena gets this per waiting row: a node of 64 bytes for the two rows of one key.
constexpr size_t ROW_LIST_BYTES_PER_WAITING_ROW = 32;
/// A key keeps its first row in its cell. Its second row allocates a node of 7 rows, and its 8th, 13th, 19th, ... row
/// another one.
constexpr size_t ROW_LIST_NODE_BYTES = 64;
constexpr size_t ROW_LIST_ROWS_PER_NODE = 6;

/// The most row-list nodes that `rows` rows allocate when `new_keys` of them start a key, in a table that then holds `keys`
/// keys. A key that gets x of the other rows gets at most 1 + x / 6 nodes more: one per key that gets any, and a sixth.
size_t maxRowListNodes(size_t rows, size_t new_keys, size_t keys)
{
    const size_t more_rows = rows - std::min(rows, new_keys);
    return std::min(keys, more_rows) + (more_rows + ROW_LIST_ROWS_PER_NODE - 1) / ROW_LIST_ROWS_PER_NODE;
}

const char * waitEndName(JoinWaitEnd reason)
{
    switch (reason)
    {
        case JoinWaitEnd::NotYet: return "not yet";
        case JoinWaitEnd::BuildFinish: return "build finish";
        case JoinWaitEnd::RowLimit: return "row limit";
        case JoinWaitEnd::ByteLimit: return "byte limit";
        case JoinWaitEnd::RepeatsOnly: return "repeats only";
        case JoinWaitEnd::RememberedVerdict: return "remembered verdict";
        case JoinWaitEnd::Backstop: return "backstop";
        case JoinWaitEnd::MemoryBound: return "memory bound";
    }
    UNREACHABLE();
}

/// Spread allowed in the projected distinct keys per partition while streams are still filling.
constexpr size_t RESERVE_MARGIN_NUMERATOR = 6;
constexpr size_t RESERVE_MARGIN_DENOMINATOR = 5;
/// A scatter copy below this takes one round: the thread starts of more rounds would cost more than the memory they save.
constexpr size_t MIN_ROUND_COPY_BYTES = 256 << 20;
/// The parallel layout fed by one stream has 256 partitions, each reserved to at least this many keys.
constexpr size_t ONE_STREAM_PARTITIONS = 256;
constexpr size_t ONE_STREAM_MIN_KEYS_PER_PARTITION = 128;

size_t zeroBufferBytes(size_t, bool)
{
    return 0;
}

/// The parallel layout fed by one stream splits a table into `ONE_STREAM_PARTITIONS` partitions. A
/// direct-addressed table has all its cells from the constructor, so it stays whole.
bool splitForOneStream(const HashJoinTable & table, bool parallel_layout)
{
    return parallel_layout && table.singleBufferBytesForGrowth() != nullptr;
}

/// What the grower of one partition map allocates for a number of keys; zero for a key type without partition maps.
HashJoinTable::BufferBytesForReserve partitionBufferBytes(const HashJoinTable & table)
{
    const auto res = table.partitionBufferBytesForReserve();
    return res ? res : &zeroBufferBytes;
}

/// A waiting record of the build stream `stream`.
struct WaitingRef
{
    size_t stream;
    WaitingBlockList::iterator it;
};

/// Every waiting record of every build stream, in arrival order (ascending `block_no`).
std::vector<WaitingRef> waitingInArrivalOrder(HashJoinBuildState & state)
{
    std::vector<WaitingRef> res;
    for (size_t s = 0; s < state.streams.size(); ++s)
        for (auto it = state.streams[s].waiting.begin(); it != state.streams[s].waiting.end(); ++it)
            res.push_back({s, it});
    std::ranges::sort(res, {}, [](const WaitingRef & w) { return w.it->block_no; });
    return res;
}

/// The bytes of a record, of the stored block it waits with, and of the block's holders.
size_t recordAndBlockBytes(const WaitingBlockRecord & record)
{
    return record.allocated_bytes + record.nullmap_bytes + (record.block_dropped ? 0 : record.stored->allocatedBytes());
}

}

size_t WaitingBlockRecord::computeAllocatedBytes() const
{
    size_t res = 0;
    for (size_t i = 0; i < key_holders.size(); ++i)
    {
        /// A key column that the stored block also holds costs nothing but the pointer.
        const bool shared_with_stored_block = stored && i < key_holder_sources.size() && key_holder_sources[i] >= 0
            && stored->columns.size() > static_cast<size_t>(key_holder_sources[i])
            && stored->columns[key_holder_sources[i]].get() == key_holders[i].get();
        if (!shared_with_stored_block)
            res += key_holders[i]->allocatedBytes();
    }
    for (size_t c = 0; c < clauses.size(); ++c)
    {
        const auto & clause = clauses[c];
        if (clause.null_map_holder)
            res += clause.null_map_holder->allocatedBytes();
        /// The mask of a condition is a column that `getColumnAsMask` allocated, or a condition column of the incoming block,
        /// which the stored block need not hold. It costs nothing only if a column held elsewhere is the same.
        if (const auto & mask = clause.join_mask.getColumn())
        {
            const auto same = [&](const ColumnPtr & column) { return column.get() == mask.get(); };
            const auto same_mask = [&](const Clause & other) { return same(other.join_mask.getColumn()); };
            const bool held_elsewhere = (stored && std::ranges::any_of(stored->columns, same)) || std::ranges::any_of(key_holders, same)
                || std::ranges::any_of(std::span(clauses).first(c), same_mask);
            if (!held_elsewhere)
                res += mask->allocatedBytes();
        }
        res += clause.skip.allocated_bytes() + clause.route.allocated_bytes() + clause.key128.allocated_bytes()
            + clause.hash64.allocated_bytes();
    }
    return res;
}

size_t HashJoinBuildState::waitingRows() const
{
    size_t res = 0;
    for (const auto & stream : streams)
        res += stream.rows.load(std::memory_order_relaxed);
    return res;
}

void HashJoin::decideBuildKind(const JoinBuildContext & context)
{
    auto & state = *data->build_state;
    join_checks_limits = context.joinChecksLimits();
    /// A parallel layout fed by one stream (`max_threads = 1`, totals on the right side) is built by one thread, as are the
    /// serial layout and the joins of wrappers.
    state.partitioned = build_layout.parallel && context.getNumStreams() > 1;

    const bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    const auto maps_kind = getMapsKind();
    bool single_row_maps = false;
    dispatchJoinShape(
        kind,
        strictness,
        maps_kind,
        [&]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
        {
            single_row_maps = std::is_same_v<Maps, MapsOne>;
            state.flags_per_cell = !flag_per_row && MapGetter<kind_, strictness_, mapsKindOf<Maps>()>::flagged;
        });
    state.drops_blocks = (maps_kind == JoinMapsKind::Set && !mustKeepRightBlocks())
        || (single_row_maps && !anyTakeLastRow() && !flag_per_row);

    if (!wait_limits_from_wrapper)
        state.wait_limits = join_checks_limits ? table_join->sizeLimits() : SizeLimits{};
    if (state.partitioned)
        state.memory_bound_bytes = 0;
    state.can_end_early = state.drops_blocks || state.wait_limits.max_rows || state.wait_limits.max_bytes || state.memory_bound_bytes;

    if (state.drops_blocks)
    {
        /// A join that can spill has a budget; one without a spill budget takes a quarter of the query's hard memory limit:
        /// `max_memory_usage`, or else the tightest limit of the user or the server.
        const size_t spill_budget = isUsedByAnotherAlgorithm() ? table_join->maxBytesBeforeExternalJoin() : 0;
        size_t budget = spill_budget ? spill_budget : table_join->getMaxMemoryUsage();
        if (!budget)
            budget = getCurrentQueryHardLimit().value_or(0);
        state.backstop_bytes = budget / BACKSTOP_DIVISOR;
    }

    if (stats_collecting_params.build.isCollectionAndUseEnabled())
    {
        if (const auto hint = getSizeHint(stats_collecting_params.build))
        {
            state.cached_keys.store(hint->ht_size, std::memory_order_relaxed);
            state.remembered_repeats_dominate = state.drops_blocks && hint->repeats_dominate;
        }
    }

    for (auto & clause : state.clauses)
        clause.merged = std::make_unique<JoinKeySketch>();
}

void HashJoin::throwIfBuildFailed() const
{
    const auto & state = *data->build_state;
    if (state.build_failed.load(std::memory_order_acquire))
    {
        if (state.failure)
            std::rethrow_exception(state.failure);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The build of the hash join failed earlier");
    }
}

void HashJoin::markBuildFailed() noexcept
{
    auto & state = *data->build_state;
    /// The first failure is the one rethrown; `failure` is written before the flag is published.
    std::lock_guard lock(state.failure_mutex);
    if (state.build_failed.load(std::memory_order_relaxed))
        return;
    state.failure = std::current_exception();
    state.build_failed.store(true, std::memory_order_release);
}

void HashJoin::checkBuildCancelled() const
{
    const auto query_context = CurrentThread::tryGetQueryContext();
    if (const auto query_status = query_context ? query_context->getProcessListElementSafe() : nullptr)
    {
        /// Throws on `KILL QUERY`, and on `max_execution_time` with `timeout_overflow_mode = 'throw'`. With `break` it returns
        /// false: the build stops here, and `buildWaitingBlocks` catches this exception because `cancelled` is set.
        if (!query_status->checkTimeLimit())
        {
            data->build_state->cancelled.store(true, std::memory_order_relaxed);
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "The build of the hash join was cancelled by the time limit");
        }
    }
    /// A cancelled pipeline: the build stops as with `break` at the time limit, and the pipeline runs no probe.
    if (const auto * flag = build_cancellation_flag.load(std::memory_order_relaxed); flag && flag->load(std::memory_order_acquire))
    {
        data->build_state->cancelled.store(true, std::memory_order_relaxed);
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "The build of the hash join was cancelled with its pipeline");
    }
}

void HashJoin::dropStoredBlock(WaitingBlockRecord & record, size_t stream)
{
    if (record.block_dropped)
        return;
    LOG_TRACE(log, "Skipping inserting block with {} rows", record.rows);
    /// A `Join` engine table has no build state.
    if (data->build_state)
        data->build_state->freed_a_block.store(true, std::memory_order_relaxed);
    data->subBytes(data->allocated_size, record.stored->allocatedBytes());
    /// A set map references no block at all, so its blocks are dropped though their rows took part in the join.
    if (getMapsKind() != JoinMapsKind::Set)
        data->rows_to_join.fetch_sub(record.rows, std::memory_order_relaxed);
    /// Null the index entry, so that a stale reference trips the chassert in `StoredColumnsIndex::at`.
    data->stored_columns_index->clearEntry(record.block_no);
    data->build_streams[stream].columns.erase(record.stored_it);
    record.stored = nullptr;
    record.block_dropped = true;
}

void HashJoin::releaseRecord(WaitingBlockList & list, WaitingBlockList::iterator it)
{
    auto & state = *data->build_state;
    data->subBytes(data->record_bytes, it->allocated_bytes);
    state.string_key_bytes.fetch_sub(it->string_key_bytes, std::memory_order_relaxed);
    state.waiting_bytes.fetch_sub(it->counted_waiting_bytes, std::memory_order_relaxed);
    list.erase(it);
}

void HashJoin::discardWaitingBlock(WaitingBlockList::iterator it, size_t stream)
{
    auto & state = *data->build_state;
    WaitingBlockRecord & record = *it;
    auto & build_stream = data->build_streams[stream];
    if (!record.block_dropped)
    {
        /// The holders point into the block; they go first.
        std::erase_if(
            build_stream.nullmaps,
            [&](const NullMapHolder & holder)
            {
                if (holder.columns != record.stored)
                    return false;
                data->subBytes(data->nullmaps_allocated_size, holder.allocatedBytes());
                return true;
            });
        data->subBytes(data->allocated_size, record.stored->allocatedBytes());
        data->stored_columns_index->clearEntry(record.block_no);
        build_stream.columns.erase(record.stored_it);
        record.stored = nullptr;
        record.block_dropped = true;
    }
    /// Nothing reads such a block, so its rows are not counted either.
    data->rows_to_join.fetch_sub(record.rows, std::memory_order_relaxed);
    state.streams[stream].rows.fetch_sub(record.rows, std::memory_order_relaxed);
    releaseRecord(state.streams[stream].waiting, it);
}

size_t HashJoin::estimatedKeysOfClause(size_t clause_idx, const char ** source, double key_scale) const
{
    const auto & state = *data->build_state;
    const size_t rows = state.waitingRows();
    /// The sketch and the cached count take every key of the disjunct, also those its table holds from earlier rounds.
    const auto new_keys = [&](size_t all_keys)
    {
        const size_t scaled = static_cast<size_t>(static_cast<double>(all_keys) * key_scale);
        const size_t built = data->tables[clause_idx]->keys();
        return std::min(rows, scaled > built ? scaled - built : 0);
    };
    if (const size_t e = state.clauses[clause_idx].estimate.load(std::memory_order_relaxed))
    {
        if (source)
            *source = "sketch";
        return new_keys(e);
    }
    /// The cached count is the keys of all disjuncts.
    if (const size_t cached = state.cached_keys.load(std::memory_order_relaxed))
    {
        if (source)
            *source = "cache";
        return new_keys(cached / state.clauses.size());
    }
    if (source)
        *source = "rows";
    return rows;
}

size_t HashJoin::estimatedKeys() const
{
    size_t res = 0;
    for (size_t clause_idx = 0; clause_idx < data->build_state->clauses.size(); ++clause_idx)
        res += estimatedKeysOfClause(clause_idx);
    return res;
}

size_t HashJoin::projectedTableBytes(bool every_row_new, bool include_built, double key_scale, bool use_final_routes) const
{
    const auto & state = *data->build_state;
    const size_t rows = state.waitingRows();
    const bool robin_hood = table_join->enableRobinHoodHashForJoins();
    const bool partitioned = state.partitioned.load(std::memory_order_relaxed);

    size_t res = 0;
    size_t row_list_nodes = 0;
    bool has_estimate = !every_row_new;
    for (size_t t = 0; t < data->tables.size(); ++t)
    {
        const auto & table = *data->tables[t];
        const char * source = "rows";
        const size_t new_keys = every_row_new ? rows : estimatedKeysOfClause(t, &source, key_scale);
        has_estimate &= std::string_view(source) != "rows";
        /// The keys of the table once the waiting rows are in.
        const size_t keys = table.keys() + new_keys;
        /// The sketch lags the rows by up to 1/16 between refreshes: only the rows it has seen tell duplicates from new keys.
        const size_t seen_rows
            = std::string_view(source) == "sketch" ? std::min(rows, state.last_refresh_rows.load(std::memory_order_relaxed)) : rows;
        row_list_nodes += maxRowListNodes(seen_rows, new_keys, keys);
        size_t bytes = 0;
        const size_t current_cells = table.publishedPartitionCapacity();
        const auto buffer_for_reserve = partitionBufferBytes(table);
        const bool one_stream_partitions = !partitioned && splitForOneStream(table, build_layout.parallel);
        if ((partitioned || one_stream_partitions) && buffer_for_reserve != &zeroBufferBytes)
        {
            size_t partitions;
            size_t initial_cells;
            if (current_cells)
            {
                partitions = table.numPartitions();
                initial_cells = current_cells;
            }
            else if (one_stream_partitions)
            {
                partitions = ONE_STREAM_PARTITIONS;
                const size_t estimate = std::min(one_stream_reserve_hint ? one_stream_reserve_hint : keys, size_t{1} << 32);
                const size_t reserve = std::max((estimate + partitions - 1) / partitions, ONE_STREAM_MIN_KEYS_PER_PARTITION);
                HashJoin::JoinPartitionGrower grower;
                grower.setRobinHoodAllowed(robin_hood);
                grower.set(reserve);
                initial_cells = std::min(grower.bufSize(), (size_t{1} << 32) / partitions);
            }
            else
            {
                const auto plan = choosePartitionPlan(
                    one_stream_reserve_hint ? one_stream_reserve_hint : keys, "rows", rows, build_layout.num_build_streams,
                    table_join->parallelHashJoinThreshold(), false, robin_hood, buffer_for_reserve);
                partitions = size_t{1} << plan.partition_bits;
                initial_cells = plan.cells_per_partition;
            }

            /// During fill, estimate the busiest partition. At the final spill decision the saved routes bound it exactly in rows.
            const size_t bounded_keys = std::min(keys, size_t{1} << 32);
            size_t fullest = (bounded_keys * RESERVE_MARGIN_NUMERATOR + partitions * RESERVE_MARGIN_DENOMINATOR - 1)
                / (partitions * RESERVE_MARGIN_DENOMINATOR);
            if (use_final_routes && (current_cells || rows >= initial_cells))
                fullest = std::min(JoinPartitionedBuild::maxPartitionKeysAtFinish(table, state, t, partitions), size_t{1} << 32);
            const size_t cells = std::min(std::max(initial_cells, std::bit_ceil(fullest + 1)), (size_t{1} << 32) / partitions);
            const size_t cell_bytes = buffer_for_reserve(0, false) / HashJoin::JoinPartitionGrower::initial_count;
            bytes = partitions * cells * cell_bytes;
            const size_t owners = std::min(partitions, build_layout.num_build_streams);
            const size_t transient = cells > initial_cells ? owners * (cells / 2) * cell_bytes : 0;
            const size_t flags = state.flags_per_cell.load(std::memory_order_relaxed) && t == 0 ? partitions * cells + 1 : 0;
            res += partitions * sizeof(void *) + std::max(flags, transient);
            if (current_cells && !include_built)
            {
                const size_t allocated = table.publishedPartitionBytes();
                res += bytes > allocated ? bytes - allocated : 0;
                continue;
            }
        }
        else if (!include_built && table.keys())
            continue;
        else if (const auto single_growth = table.singleBufferBytesForGrowth())
            /// The single map, LowCardinality maps included, grows from the reserve that the constructor made.
            bytes = single_growth(keys, one_stream_reserve_hint);
        /// A direct-addressed map has all its cells from the constructor.

        /// Only what goes beyond the buffer that the constructor allocated: the allocated bytes hold that already.
        res += bytes > initial_table_bytes[t] ? bytes - initial_table_bytes[t] : 0;
    }

    /// The arenas take the characters of the new `String` keys, at most those of every waiting row, and the row lists of a
    /// map that keeps every row of a key: also of a plain `ALL` join, whose maps kind is `Default`, once there is an
    /// estimate. Without one, only a join promoted to `JoinMapsKind::All` is charged the upper bound per row: charged to every
    /// `ALL` join, it would keep a `GraceHashJoin` bucket above a small spill threshold however often the bucket splits.
    const size_t string_bytes = state.string_key_bytes.load(std::memory_order_acquire);
    size_t arena_bytes = string_bytes;
    if (data->tables.front()->holds<MapsAll>())
    {
        if (has_estimate)
            arena_bytes += row_list_nodes * ROW_LIST_NODE_BYTES;
        else if (getMapsKind() == JoinMapsKind::All)
            arena_bytes += rows * ROW_LIST_BYTES_PER_WAITING_ROW;
    }
    if (arena_bytes)
    {
        const size_t streams = partitioned
            ? std::clamp<size_t>(rows / JoinPartitionedBuild::MIN_ROWS_PER_OWNER, 1, build_layout.num_build_streams)
            : build_layout.num_build_streams;
        for (const auto & table : data->tables)
            res += table->nextArenaChunks(streams);
        if (string_bytes)
        {
            /// An allocation can abandon the tail of its chunk when the next string is larger than the space left.
            /// Account for all owners together. Each extra arena can round its last chunk to a page and pad it separately.
            /// Serial replay uses only the streams with waiting rows. A grace bucket can receive rows from just one.
            const size_t owners = partitioned ? streams : std::max<size_t>(1, std::ranges::count_if(
                state.streams, [](const auto & stream) { return stream.rows.load(std::memory_order_relaxed) != 0; }));
            const size_t arenas = owners * data->tables.size();
            const size_t rounding = (arenas - 1) * 2 * static_cast<size_t>(getPageSize());
            const size_t node_bytes = arena_bytes - string_bytes;
            const size_t nodes = node_bytes / ROW_LIST_NODE_BYTES + (node_bytes % ROW_LIST_NODE_BYTES != 0);
            /// A preceding string can leave the next row-list node unaligned.
            const size_t padding = rounding + nodes * alignof(RowRefList::Batch);
            constexpr size_t max_size = std::numeric_limits<size_t>::max();
            const size_t allocations = use_final_routes ? rows * data->tables.size() + nodes : max_size;
            const size_t growth = arena_bytes <= max_size - padding ? Arena{}.estimateGrowthMemory(allocations, arena_bytes + padding) : max_size;
            if (growth >= max_size - res)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Hash join arena size estimate exceeds the address space");
            res += growth;
        }
        else
            res += 2 * arena_bytes;
    }
    return res;
}

size_t HashJoin::waitByteBound() const
{
    return getTotalByteCount() + projectedTableBytes(/*every_row_new=*/true);
}

size_t HashJoin::roundCopyCap() const
{
    return std::max({projectedTableBytes(/*every_row_new=*/false, /*include_built=*/true), getTotalByteCount() / 4, MIN_ROUND_COPY_BYTES});
}

size_t HashJoin::scatterCopyBytes(size_t rows, size_t string_key_bytes) const
{
    /// Only the `String` maps copy the characters; a packed key holds them in its prepared key.
    const bool copies_characters = keyType() == Type::key_string || keyType() == Type::key_fixed_string;
    return rows * data->tables.front()->scatterBytesPerRow(mustKeepRightBlocks()) + (copies_characters ? string_key_bytes : 0);
}

size_t HashJoin::getProjectedCopyBytes() const
{
    const auto * state = data ? data->build_state.get() : nullptr;
    if (!state || !state->partitioned.load(std::memory_order_relaxed) || !isWaiting())
        return 0;
    const size_t copy = scatterCopyBytes(state->waitingRows(), state->string_key_bytes.load(std::memory_order_relaxed));
    return std::min(copy, roundCopyCap());
}

size_t HashJoin::getProjectedByteCount() const
{
    if (!data)
        return 0;
    const size_t allocated = getTotalByteCount();
    if (!data->build_state || !isWaiting())
        return allocated;
    return allocated + projectedTableBytes(/*every_row_new=*/false);
}

void HashJoin::setWaitMemoryBound(size_t bytes)
{
    if (data->build_state)
        data->build_state->memory_bound_bytes = bytes;
}

void HashJoin::setWaitLimits(const SizeLimits & limits)
{
    if (!data->build_state)
        return;
    data->build_state->wait_limits = limits;
    wait_limits_from_wrapper = true;
}

bool HashJoin::isWaiting() const
{
    const auto * state = data ? data->build_state.get() : nullptr;
    if (!state || state->finished)
        return false;
    return state->waitingRows() != 0 || !state->built_early;
}

bool HashJoin::builtEarly() const
{
    const auto * state = data ? data->build_state.get() : nullptr;
    return state && state->built_early;
}

HashJoin::BuildFit HashJoin::evaluateBuildFit(size_t budget)
{
    const auto & state = *data->build_state;
    grouped_budget = 0;
    const size_t base = getTotalByteCount();
    const size_t rows = state.waitingRows();
    /// A table of one stream has no partition plan and no rounds: it fits if the held bytes and its projection stay below the
    /// budget.
    if (!state.partitioned)
        return base + projectedTableBytes(/*every_row_new=*/false, /*include_built=*/false, 1., /*use_final_routes=*/true) < budget ? BuildFit::Fits : BuildFit::MustSpill;

    /// The estimate refreshes only after the rows grow by 1/16, so it can lag them, and the projected tables double at a
    /// power-of-two key count: a lagging estimate could keep in memory a build that must spill. Every stream has finished,
    /// so nothing else merges.
    mergeEverySketch();
    const size_t tables = projectedTableBytes(/*every_row_new=*/false, /*include_built=*/false, 1., /*use_final_routes=*/true);

    /// The scatter copy, at most `roundCopyCap` per round. The disjuncts build one after the other, so the largest partition
    /// count divides.
    const size_t copy = std::min(scatterCopyBytes(rows, state.string_key_bytes.load(std::memory_order_relaxed)), roundCopyCap());
    size_t partitions = 1;
    for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
    {
        const auto plan = choosePartitionPlan(
            estimatedKeysOfClause(clause_idx),
            "rows",
            rows,
            build_layout.num_build_streams,
            table_join->parallelHashJoinThreshold(),
            /*early=*/false,
            table_join->enableRobinHoodHashForJoins(),
            partitionBufferBytes(*data->tables[clause_idx]));
        partitions = std::max(partitions, size_t{1} << plan.partition_bits);
    }
    const size_t at_once = base + std::max(copy + tables / partitions, tables + copy / partitions);
    size_t max_block_copy = 0;
    for (const auto & stream : state.streams)
        for (const auto & record : stream.waiting)
            max_block_copy = std::max(max_block_copy, scatterCopyBytes(record.rows, record.string_key_bytes));
    const size_t in_rounds = base + tables + max_block_copy;
    LOG_TRACE(
        log,
        "{}Build fit: held = {}, tables = {}, copy = {}, at once = {}, in rounds = {}, budget = {}",
        instance_log_id,
        base,
        tables,
        copy,
        at_once,
        in_rounds,
        budget);

    if (at_once <= budget)
        return BuildFit::Fits;
    if (in_rounds <= budget && !state.built_early)
    {
        grouped_budget = budget;
        return BuildFit::Grouped;
    }
    return BuildFit::MustSpill;
}

void HashJoin::startSketch(size_t stream_idx)
{
    auto & state = *data->build_state;
    auto & stream = state.streams[stream_idx];
    if (stream.sketches.empty() || stream.sketches.front().load(std::memory_order_relaxed))
        return;
    stream.sketch_storage.resize(state.clauses.size());
    for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
    {
        auto sketch = std::make_unique<JoinKeySketch>();
        for (const auto & record : stream.waiting)
            JoinPartitionedBuild::sketchClause(
                *data->tables[clause_idx],
                strictness == JoinStrictness::Asof,
                record.clauses[clause_idx],
                record.stored->selector,
                *sketch);
        stream.sketch_storage[clause_idx] = std::move(sketch);
    }
    /// Published after it fed the rows the stream already holds.
    for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
        stream.sketches[clause_idx].store(stream.sketch_storage[clause_idx].get(), std::memory_order_release);
}

void HashJoin::liveTest(size_t rows)
{
    auto & state = *data->build_state;
    if (!state.drops_blocks || rows < LIVE_TEST_MIN_ROWS)
    {
        for (auto & clause : state.clauses)
        {
            clause.prev_rows = rows;
            clause.prev_estimate = clause.estimate.load(std::memory_order_relaxed);
        }
        return;
    }

    /// The block is dropped only when no table points into it, so keys must have stopped arriving in every table.
    bool repeats_only = true;
    bool all_baselines = true;
    for (auto & clause : state.clauses)
    {
        const size_t e = clause.merged->size();
        if (clause.baseline_taken)
        {
            /// Signed: an estimate can step down a little.
            const Int64 new_keys = static_cast<Int64>(e) - static_cast<Int64>(clause.prev_estimate);
            const Int64 new_rows = static_cast<Int64>(rows - clause.prev_rows);
            repeats_only &= new_keys * static_cast<Int64>(REPEATS_ONLY_RATIO) < new_rows;
        }
        else
            all_baselines = false;
        clause.baseline_taken = true;
        clause.prev_rows = rows;
        clause.prev_estimate = e;
    }
    if (all_baselines && repeats_only)
        state.end_wait.store(true, std::memory_order_relaxed);
}

void HashJoin::maybeRefreshEstimate()
{
    auto & state = *data->build_state;
    if (state.waitingRows() * REFRESH_GROWTH_DIVISOR < state.last_refresh_rows.load(std::memory_order_relaxed) * (REFRESH_GROWTH_DIVISOR + 1))
        return;
    /// A try-lock. The losers skip: the holder merges every sketch.
    if (state.refreshing.exchange(true, std::memory_order_acquire))
        return;
    SCOPE_EXIT({ state.refreshing.store(false, std::memory_order_release); });

    /// Read again under the flag: another refresh may have run since the check above.
    const size_t last_rows = state.last_refresh_rows.load(std::memory_order_relaxed);
    const size_t rows = state.waitingRows();
    if (rows * REFRESH_GROWTH_DIVISOR < last_rows * (REFRESH_GROWTH_DIVISOR + 1))
        return;

    for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
    {
        auto & clause = state.clauses[clause_idx];
        for (auto & stream : state.streams)
            if (auto * sketch = stream.sketches[clause_idx].load(std::memory_order_acquire); sketch && sketch->consumeChange())
                clause.merged->mergeLive(*sketch);
        /// The denominator is computed here, not on every insert.
        clause.estimate.store(clause.merged->size(), std::memory_order_relaxed);
    }
    liveTest(rows);
    state.last_refresh_rows.store(rows, std::memory_order_relaxed);
}

void HashJoin::mergeEverySketch()
{
    auto & state = *data->build_state;
    state.last_refresh_rows.store(state.waitingRows(), std::memory_order_relaxed);
    for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
    {
        auto & clause = state.clauses[clause_idx];
        for (auto & stream : state.streams)
            if (auto * sketch = stream.sketches[clause_idx].load(std::memory_order_acquire))
                clause.merged->mergeLive(*sketch);
        clause.estimate.store(clause.merged->size(), std::memory_order_relaxed);
    }
}

JoinWaitEnd HashJoin::checkWaitBounds() const
{
    const auto & state = *data->build_state;
    /// The verdict is about the first wait: the batches after an early build end by the other rules.
    if (state.remembered_repeats_dominate && !state.built_early)
        return JoinWaitEnd::RememberedVerdict;
    if (state.end_wait.load(std::memory_order_relaxed))
        return JoinWaitEnd::RepeatsOnly;

    const size_t rows = state.waitingRows();
    /// Each disjunct has at most one key per row, so before this point the row limit cannot be crossed.
    if (state.wait_limits.max_rows && data->keys() + rows * state.clauses.size() >= state.wait_limits.max_rows)
        return JoinWaitEnd::RowLimit;
    /// An upper bound: every waiting row counted as a new key, whatever an estimate says.
    if (state.wait_limits.max_bytes && waitByteBound() >= state.wait_limits.max_bytes)
        return JoinWaitEnd::ByteLimit;
    /// The bytes of the current batch only, not of the blocks that earlier rounds kept.
    if (state.drops_blocks && state.backstop_bytes && state.waiting_bytes.load(std::memory_order_relaxed) >= state.backstop_bytes)
        return JoinWaitEnd::Backstop;
    if (state.memory_bound_bytes && waitByteBound() >= state.memory_bound_bytes)
        return JoinWaitEnd::MemoryBound;
    return JoinWaitEnd::NotYet;
}

JoinWaitEnd HashJoin::afterBlockWaits(size_t stream_idx)
{
    auto & state = *data->build_state;
    auto & stream = state.streams[stream_idx];

    if (!state.partitioned)
    {
        /// The lazy sketch of a table of one build stream: only when something reads the estimate.
        const bool has_reader = sketch_from_first_block || state.drops_blocks || wait_limits_from_wrapper
            || state.wait_limits.max_rows || state.wait_limits.max_bytes || isUsedByAnotherAlgorithm();
        if (has_reader && !stream.sketches.front().load(std::memory_order_relaxed))
        {
            const size_t rows = state.waitingRows();
            const size_t byte_bound = waitByteBound();
            const size_t budget = table_join->maxBytesBeforeExternalJoin();
            const bool start = sketch_from_first_block || rows >= table_join->parallelHashJoinThreshold()
                || (budget && byte_bound >= budget / LAZY_SKETCH_DIVISOR)
                || (state.wait_limits.max_bytes && byte_bound >= state.wait_limits.max_bytes / LAZY_SKETCH_DIVISOR);
            if (start)
                startSketch(stream_idx);
        }
        else if (stream.sketches.front().load(std::memory_order_relaxed))
        {
            /// The newest record is not sketched yet.
            const auto & record = stream.waiting.back();
            for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
                JoinPartitionedBuild::sketchClause(
                    *data->tables[clause_idx],
                    strictness == JoinStrictness::Asof,
                    record.clauses[clause_idx],
                    record.stored->selector,
                    *stream.sketch_storage[clause_idx]);
        }
    }

    maybeRefreshEstimate();
    return checkWaitBounds();
}

bool HashJoin::checkSizeLimitsAfterBuild(size_t bytes)
{
    if (!join_checks_limits || wait_limits_from_wrapper)
        return true;
    updatePeakBuildBytes(bytes);
    if (table_join->sizeLimits().check(data->keys(), bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return true;
    data->build_state->stop_reading.store(true, std::memory_order_relaxed);
    return false;
}

bool HashJoin::replayOneStream()
{
    auto & state = *data->build_state;

    /// A grace bucket is fed through several stream ids, but serializes its inserts: replay in arrival order.
    const auto order = waitingInArrivalOrder(state);
    size_t pending_bytes = 0;
    for (const auto & [_, it] : order)
        pending_bytes += recordAndBlockBytes(*it);

    /// An empty build keeps the one empty table of the constructor.
    if (!state.built_early && !order.empty())
    {
        const size_t hint = one_stream_reserve_hint;
        for (size_t t = 0; t < data->tables.size(); ++t)
        {
            auto & table = data->tables[t];
            if (splitForOneStream(*table, build_layout.parallel) && !table->isPartitioned())
            {
                const size_t estimate = std::min(hint ? hint : estimatedKeysOfClause(t), size_t{1} << 32);
                const size_t mean = (estimate + ONE_STREAM_PARTITIONS - 1) / ONE_STREAM_PARTITIONS;
                table->usePartitions(8);
                table->setPartitionReserveOnFirstInsert(std::max(mean, ONE_STREAM_MIN_KEYS_PER_PARTITION));
            }
            /// The hint, once per table that gets rows; the serial table reserved it in the constructor.
            ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, hint);
        }
    }

    for (size_t i = 0; i < order.size(); ++i)
    {
        const auto & [stream, it] = order[i];
        WaitingBlockRecord & record = *it;
        const size_t record_block_bytes = recordAndBlockBytes(record);
        const bool referenced = insertRecord(record, stream);
        if (!referenced)
            dropStoredBlock(record, stream);
        state.streams[stream].rows.fetch_sub(record.rows, std::memory_order_relaxed);
        releaseRecord(state.streams[stream].waiting, it);
        pending_bytes -= record_block_bytes;

        checkBuildCancelled();

        /// The check reads the bytes of the blocks replayed so far, not of all waiting blocks.
        if (!checkSizeLimitsAfterBuild(getTotalByteCount() - std::min(pending_bytes, getTotalByteCount())))
        {
            /// `break`: an insert on arrival would stop reading at the block that crossed the limit, so the blocks after it go.
            for (size_t j = i + 1; j < order.size(); ++j)
                discardWaitingBlock(order[j].it, order[j].stream);
            return false;
        }
    }
    return true;
}

bool HashJoin::runPartitionedRound(JoinWaitEnd reason)
{
    auto & state = *data->build_state;
    const bool early = reason != JoinWaitEnd::BuildFinish;

    const auto waiting = waitingInArrivalOrder(state);
    if (waiting.empty())
        return true;
    size_t rows = 0;
    std::vector<WaitingBlockRecord *> records;
    records.reserve(waiting.size());
    for (const auto & w : waiting)
    {
        rows += w.it->rows;
        records.push_back(&*w.it);
    }

    /// A wrapped set map keeps the blocks that added keys for replay if the join spills.
    const bool track_referenced_blocks = state.drops_blocks || getMapsKind() == JoinMapsKind::Set;
    if (track_referenced_blocks)
    {
        const size_t num_blocks = data->stored_columns_index->size();
        if (state.referenced_size < num_blocks)
        {
            auto referenced = std::make_unique<std::atomic<UInt8>[]>(num_blocks);
            for (size_t i = 0; i < num_blocks; ++i)
                referenced[i].store(i < state.referenced_size ? state.referenced[i].load(std::memory_order_relaxed) : 0, std::memory_order_relaxed);
            state.referenced = std::move(referenced);
            state.referenced_size = num_blocks;
        }
    }

    mergeEverySketch();

    /// Each disjunct's table is planned from that disjunct's estimate.
    std::vector<JoinPartitionPlan> plans;
    size_t estimate = 0;
    if (!state.built_early)
    {
        for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
        {
            const char * source = "rows";
            size_t clause_estimate = estimatedKeysOfClause(clause_idx, &source);
            /// The cached key count of a join with one disjunct, or `reserve_num`, sizes the tables: the sketch only serves the
            /// wait. `HashJoinPreallocatedElementsInHashTables` counts that hint, once per table.
            if (!early && one_stream_reserve_hint)
            {
                clause_estimate = one_stream_reserve_hint;
                source = "cache";
                ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, one_stream_reserve_hint);
            }
            estimate += clause_estimate;
            plans.push_back(choosePartitionPlan(
                clause_estimate,
                source,
                rows,
                build_layout.num_build_streams,
                table_join->parallelHashJoinThreshold(),
                early,
                table_join->enableRobinHoodHashForJoins(),
                partitionBufferBytes(*data->tables[clause_idx])));
            logBuild(reason, &plans.back(), rows);
        }
    }

    /// A `Grouped` fit: rounds over consecutive ranges of blocks, each as large as the free budget allows. Any build also
    /// keeps the scatter copy of a round within `roundCopyCap`. The first round creates and reserves the tables from the plan.
    const size_t budget = !early && !plans.empty() ? std::exchange(grouped_budget, 0) : 0;
    const size_t copy_cap = roundCopyCap();
    std::vector<size_t> record_copy(records.size());
    size_t copy_to_come = 0;
    for (size_t i = 0; i < records.size(); ++i)
        copy_to_come += record_copy[i] = scatterCopyBytes(records[i]->rows, records[i]->string_key_bytes);

    const auto maps_kind = getMapsKind();
    size_t begin = 0;
    size_t inserted_rows = 0;
    while (begin < records.size())
    {
        size_t end = records.size();
        std::optional<size_t> range_limit;
        if (budget)
        {
            /// The final key count: the sketch's estimate while the inserted keys stay below 1.2 times it; above that the
            /// sketch was too low, and the keys inserted so far are extrapolated over all rows.
            const size_t inserted_keys = data->keys();
            double key_scale = 1.0;
            if (inserted_rows && estimate && inserted_keys * RESERVE_MARGIN_DENOMINATOR >= estimate * RESERVE_MARGIN_NUMERATOR)
                key_scale = static_cast<double>(inserted_keys) * static_cast<double>(rows) / static_cast<double>(inserted_rows)
                    / static_cast<double>(estimate);
            const size_t tables_to_come = projectedTableBytes(/*every_row_new=*/false, /*include_built=*/true, key_scale);
            const size_t used = getTotalByteCount() + (tables_to_come > data->bucket_bytes ? tables_to_come - data->bucket_bytes : 0);
            range_limit = budget > used ? budget - used : 0;
        }
        else if (copy_to_come > copy_cap)
            range_limit = copy_cap;
        if (range_limit)
        {
            size_t range_bytes = record_copy[begin];
            end = begin + 1;
            while (end < records.size() && range_bytes + record_copy[end] <= *range_limit)
                range_bytes += record_copy[end++];
        }
        const std::span<WaitingBlockRecord * const> range(records.data() + begin, end - begin);

        for (size_t clause_idx = 0; clause_idx < state.clauses.size(); ++clause_idx)
        {
            dispatchJoinShape(
                kind,
                strictness,
                maps_kind,
                [&]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
                {
                    const JoinPartitionedBuild::RoundSettings settings{
                        .any_take_last_row = anyTakeLastRow(),
                        .drops_blocks = state.drops_blocks,
                        .keep_right_blocks = mustKeepRightBlocks(),
                        .asof_type = getAsofType(),
                        .asof_inequality = getAsofInequality(),
                        .plan = begin == 0 && !plans.empty() ? &plans[clause_idx] : nullptr,
                        .max_threads = build_layout.num_build_streams,
                        .enable_software_prefetch = enableSoftwarePrefetch()};
                    const BuildResult result = JoinPartitionedBuild::buildRound<Maps>(
                        *data->tables[clause_idx],
                        clause_idx,
                        range,
                        settings,
                        state,
                        [&](size_t bytes) { data->addBytes(data->bucket_bytes, bytes); },
                        [&] { checkBuildCancelled(); });
                    if (!result.all_values_unique)
                        all_values_unique.store(false, std::memory_order_relaxed);
                });
            data->tables[clause_idx]->noteNextArenaChunks();
        }

        ProfileEvents::increment(ProfileEvents::HashJoinBuildRounds);

        /// After the owners: drop the blocks that nothing points at, then free the records.
        for (size_t i = begin; i < end; ++i)
        {
            const auto & [stream, it] = waiting[i];
            WaitingBlockRecord & record = *it;
            const bool referenced = record.keeps_block || record.block_dropped
                || std::ranges::any_of(record.clauses, [](const auto & clause) { return clause.skipped_null_key; })
                || !track_referenced_blocks || state.referenced[record.block_no].load(std::memory_order_relaxed);
            if (!referenced)
                dropStoredBlock(record, stream);
            inserted_rows += record.rows;
            copy_to_come -= record_copy[i];
            state.streams[stream].rows.fetch_sub(record.rows, std::memory_order_relaxed);
            releaseRecord(state.streams[stream].waiting, it);
        }
        begin = end;
        updatePeakBuildBytes(getTotalByteCount());
    }

    /// The live test starts again for the next batch: its baseline is taken anew.
    for (auto & clause : state.clauses)
        clause.baseline_taken = false;
    state.end_wait.store(false, std::memory_order_relaxed);
    state.last_refresh_rows.store(0, std::memory_order_relaxed);

    /// One exact check of the size limits after every round, the last one included.
    return checkSizeLimitsAfterBuild(getTotalByteCount());
}

bool HashJoin::buildWaitingBlocks(JoinWaitEnd reason)
{
    auto & state = *data->build_state;
    if (state.wait_end == JoinWaitEnd::NotYet)
        state.wait_end = reason;

    bool ok = true;
    try
    {
        ok = state.partitioned ? runPartitionedRound(reason) : replayOneStream();
    }
    catch (...)
    {
        /// `timeout_overflow_mode = 'break'`: the build stops, and the executor ends the query before any probe.
        if (state.cancelled.load(std::memory_order_relaxed))
        {
            state.stop_reading.store(true, std::memory_order_relaxed);
            return false;
        }
        markBuildFailed();
        throw;
    }

    if (!state.partitioned && !state.built_early)
        logBuild(reason, nullptr, data->rows_to_join.load(std::memory_order_relaxed));
    if (reason != JoinWaitEnd::BuildFinish)
        state.built_early = true;
    return ok;
}

void HashJoin::logBuild(JoinWaitEnd reason, const JoinPartitionPlan * plan, size_t rows) const
{
    const auto & state = *data->build_state;
    if (plan)
        LOG_TRACE(
            log,
            "{}Partitioned build: P = {}, E = {} ({}), rows = {}, cells = {} per partition, Robin Hood = {}, the wait ended at {}{}",
            instance_log_id,
            size_t{1} << plan->partition_bits,
            plan->estimate,
            plan->estimate_source,
            rows,
            plan->cells_per_partition,
            plan->robin_hood,
            waitEndName(reason),
            state.backstop_bytes || !state.drops_blocks ? "" : ", no backstop");
    else
        LOG_TRACE(
            log,
            "{}{} build of one stream: rows = {}, the wait ended at {}{}",
            instance_log_id,
            data->tables.front()->isPartitioned() ? "256-partition" : "One-table",
            rows,
            waitEndName(reason),
            state.backstop_bytes || !state.drops_blocks ? "" : ", no backstop");
}

}
