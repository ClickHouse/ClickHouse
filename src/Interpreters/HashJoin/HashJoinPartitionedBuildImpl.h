#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <Interpreters/HashJoin/HashJoinPartitionedBuild.h>
#include <Interpreters/HashJoin/HashJoinPartitionedBuildKeys.h>
#include <Interpreters/RowRefs.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/getL2CacheSize.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <array>
#include <bit>
#include <numeric>

namespace ProfileEvents
{
extern const Event HashJoinBuildPartitions;
extern const Event HashJoinPartitionTableResizes;
extern const Event HashJoinBuildScatterMicroseconds;
extern const Event HashJoinBuildInsertMicroseconds;
extern const Event JoinPackedStringKeysInline;
extern const Event JoinPackedStringKeysHashed;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace JoinPartitionedBuild::detail
{

using ColumnsScatter::scatterPidChunk;
using ColumnsScatter::ScatterScratch;
using ColumnsScatter::StoreMode;

/// The row locator of the scatter: `(block_no << 16) | row` in 4 bytes when it fits, otherwise the encoded `RowRef`.
struct Locator
{
    UInt32 block_no;
    UInt32 row;
};

template <bool narrow>
ALWAYS_INLINE inline Locator readLocator(const char * locators, size_t i)
{
    if constexpr (narrow)
    {
        UInt32 word = 0;
        memcpy(&word, locators + i * sizeof(UInt32), sizeof(word));
        return {word >> 16, word & 0xFFFF};
    }
    UInt64 word = 0;
    memcpy(&word, locators + i * sizeof(UInt64), sizeof(word));
    return {refWordBlockNo(word), refWordRowNo(word)};
}

/// One fixed-width column of every piece: the memory of piece `p` is `data[p]`.
struct FixedPieces
{
    size_t width = 0;
    StoreMode mode = StoreMode::Direct;
    std::vector<MutableColumnPtr> columns;
    std::vector<char *> data;

    void allocate(const IColumn & sample, std::span<const size_t> rows)
    {
        width = sample.sizeOfValueIfFixed();
        columns.resize(rows.size());
        data.resize(rows.size());
        for (size_t p = 0; p < rows.size(); ++p)
        {
            auto [column, bytes] = ColumnsScatter::allocateUninitializedFixed(sample, rows[p]);
            columns[p] = std::move(column);
            data[p] = bytes.data();
        }
    }

    void release(size_t p)
    {
        columns[p] = nullptr;
        data[p] = nullptr;
    }

    [[nodiscard]] bool empty() const { return width == 0; }
};

template <HashJoin::Type TYPE, typename Maps>
BuildResult buildRoundTyped(
    HashJoinTable & table,
    size_t clause_idx,
    std::span<WaitingBlockRecord * const> blocks,
    const RoundSettings & settings,
    HashJoinBuildState & state,
    absl::FunctionRef<void(size_t)> add_bytes,
    absl::FunctionRef<void()> check_cancelled)
{
    constexpr KeyKind kind = keyKindOf<TYPE>();
    using Map = JoinPartMapOf<TYPE, Maps>;
    using Key = typename Map::key_type;
    using Mapped = typename Maps::MappedType;
    constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
    const bool has_locator = Maps::has_mapped || settings.keep_right_blocks;
    constexpr bool fixed_key = kind != KeyKind::String;

    /// The first round switches the table to its partitions.
    if (settings.plan)
    {
        if (table.isPartitioned())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The first round of a partitioned build found a table with partitions");
        table.usePartitions(settings.plan->partition_bits);
        table.template partitionedView<TYPE, Maps>().configureCommonCapacity(settings.plan->cells_per_partition, settings.plan->robin_hood);
        ProfileEvents::increment(ProfileEvents::HashJoinBuildPartitions, table.numPartitions());
    }
    else if (!table.isPartitioned())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A later round of a partitioned build found a table without partitions");

    const size_t num_partitions = table.numPartitions();
    const size_t partition_bits = std::countr_zero(num_partitions);
    chassert(std::has_single_bit(num_partitions) && partition_bits >= 1 && partition_bits <= 15);
    /// Id `num_partitions` is the extra piece: the rows that are not inserted. No owner reads it.
    const size_t fanout = num_partitions + 1;
    const size_t extra = num_partitions;
    const unsigned shift = static_cast<unsigned>(16 - partition_bits);

    auto clause_of = [&](size_t b) -> WaitingBlockRecord::Clause & { return blocks[b]->clauses[clause_idx]; };

    size_t total_rows = 0;
    /// The 4-byte locator holds a block number below 65536 and a row number below 65536.
    bool narrow_locators = true;
    for (size_t b = 0; b < blocks.size(); ++b)
    {
        const size_t rows = clause_of(b).route.size();
        total_rows += rows;
        narrow_locators = narrow_locators && blocks[b]->block_no < (1u << 16) && rows <= (1u << 16);
    }

    BuildResult result;
    if (blocks.empty())
        return result;

    std::atomic<bool> stop = false;

    /// The stripes are runs of whole blocks with about equal rows. Both passes of the scatter use the same stripes.
    const size_t scatter_workers = std::max<size_t>(1, std::min(settings.max_threads, blocks.size()));
    std::vector<size_t> stripe_begin{0};
    {
        const size_t target = (total_rows + scatter_workers - 1) / scatter_workers;
        size_t rows = 0;
        for (size_t b = 0; b + 1 < blocks.size(); ++b)
        {
            rows += clause_of(b).route.size();
            if (rows >= target * stripe_begin.size())
                stripe_begin.push_back(b + 1);
        }
        stripe_begin.push_back(blocks.size());
    }
    const size_t num_stripes = stripe_begin.size() - 1;

    Stopwatch scatter_watch;

    /// The counting pass: turn each route into its partition id in place, and count the rows of every piece per stripe.
    std::vector<UInt64> hist(num_stripes * fanout, 0);
    {
        std::atomic<size_t> next_stripe = 0;
        runWorkers(std::min(scatter_workers, num_stripes), stop, [&](size_t)
        {
            const bool interleave = fanout <= ColumnsScatter::HIST_INTERLEAVE_MAX_FANOUT;
            PODArray<UInt64> lanes(interleave ? 4 * fanout : 0);
            size_t s = 0;
            while (!stop.load(std::memory_order_relaxed) && (s = next_stripe.fetch_add(1)) < num_stripes)
            {
                UInt64 * stripe_hist = hist.data() + s * fanout;
                std::fill(lanes.begin(), lanes.end(), 0);
                for (size_t b = stripe_begin[s]; b < stripe_begin[s + 1]; ++b)
                {
                    auto & clause = clause_of(b);
                    UInt16 * pids = clause.route.data();
                    const size_t rows = clause.route.size();
                    if (!clause.skip.empty())
                    {
                        const UInt8 * skip = clause.skip.data();
                        for (size_t i = 0; i < rows; ++i)
                            pids[i] = skip[i] ? static_cast<UInt16>(extra) : static_cast<UInt16>(pids[i] >> shift);
                    }
                    else
                    {
                        for (size_t i = 0; i < rows; ++i)
                            pids[i] = static_cast<UInt16>(pids[i] >> shift);
                    }
                    ColumnsScatter::histogramPidChunk(pids, rows, stripe_hist, interleave ? lanes.data() : nullptr, fanout);
                    check_cancelled();
                }
                if (interleave)
                    ColumnsScatter::reduceHistogramLanes(stripe_hist, lanes.data(), fanout);
            }
        });
    }

    /// Exact sizes, and the start of every stripe in every piece.
    std::vector<size_t> piece_rows(fanout, 0);
    std::vector<size_t> stripe_start(num_stripes * fanout);
    for (size_t p = 0; p < fanout; ++p)
        for (size_t s = 0; s < num_stripes; ++s)
        {
            stripe_start[s * fanout + p] = piece_rows[p];
            piece_rows[p] += hist[s * fanout + p];
        }

    /// Write combining only pays for pieces of about equal size.
    /// TODO: tune the skew threshold on x86 with uniform and Zipf keys.
    const size_t largest_piece = *std::max_element(piece_rows.begin(), piece_rows.begin() + num_partitions);
    const bool allow_combining = largest_piece * num_partitions <= 4 * total_rows;

    FixedPieces key_pieces;
    FixedPieces locator_pieces;
    FixedPieces hash_pieces;
    FixedPieces asof_pieces;
    if constexpr (fixed_key)
        key_pieces.allocate(*ColumnVector<Key>::create(), piece_rows);
    else
        hash_pieces.allocate(*ColumnUInt64::create(), piece_rows);
    if (has_locator)
    {
        if (!Maps::has_mapped || narrow_locators)
            locator_pieces.allocate(*ColumnUInt32::create(), piece_rows);
        else
            locator_pieces.allocate(*ColumnUInt64::create(), piece_rows);
    }
    if constexpr (is_asof_join)
        asof_pieces.allocate(*clause_of(0).key_columns.back()->cloneEmpty(), piece_rows);
    const std::array<FixedPieces *, 4> all_pieces{&key_pieces, &locator_pieces, &hash_pieces, &asof_pieces};
    for (FixedPieces * pieces : all_pieces)
        if (!pieces->empty())
            pieces->mode = ColumnsScatter::chooseStoreMode(pieces->width, fanout, total_rows, allow_combining);

    /// `String` keys: per stripe, one column per piece from the multi-source scatter.
    std::vector<VectorWithMemoryTracking<MutableColumnPtr>> string_pieces(fixed_key ? 0 : num_stripes);

    /// The copy pass: copy the keys, the saved hashes, the `ASOF` values and the locators into the pieces.
    {
        std::atomic<size_t> next_stripe = 0;
        const size_t batch_rows = ColumnsScatter::scatterBatchRowsTarget(fanout);
        runWorkers(std::min(scatter_workers, num_stripes), stop, [&](size_t)
        {
            /// A temporary of 32768 rows stays in the cache.
            static constexpr size_t chunk_rows = 32768;
            PODArray<UInt64> locator_chunk(has_locator ? chunk_rows : 0);
            PODArray<Key> packed_chunk(kind == KeyKind::Packed ? chunk_rows : 0);
            ScatterScratch key_scratch;
            ScatterScratch locator_scratch;
            ScatterScratch hash_scratch;
            ScatterScratch asof_scratch;
            const std::array<std::pair<FixedPieces *, ScatterScratch *>, 4> columns{{
                {&key_pieces, &key_scratch}, {&locator_pieces, &locator_scratch}, {&hash_pieces, &hash_scratch},
                {&asof_pieces, &asof_scratch}}};
            for (auto [pieces, scratch] : columns)
                if (!pieces->empty())
                    scratch->init(fanout, pieces->mode);

            size_t s = 0;
            while (!stop.load(std::memory_order_relaxed) && (s = next_stripe.fetch_add(1)) < num_stripes)
            {
                const UInt64 * stripe_hist = hist.data() + s * fanout;
                for (auto [pieces, scratch] : columns)
                    if (!pieces->empty())
                        for (size_t p = 0; p < fanout; ++p)
                            if (stripe_hist[p])
                                scratch->seed(p, pieces->data[p] + stripe_start[s * fanout + p] * pieces->width);

                if constexpr (!fixed_key)
                {
                    /// The string scatter comes first, because the runs below free the routes.
                    std::vector<const IColumn *> sources;
                    std::vector<std::span<const UInt16>> ids;
                    for (size_t b = stripe_begin[s]; b < stripe_begin[s + 1]; ++b)
                    {
                        sources.push_back(clause_of(b).key_columns.at(0));
                        ids.emplace_back(clause_of(b).route.data(), clause_of(b).route.size());
                    }
                    /// `UInt64` is not `size_t` on every platform.
                    const std::vector<size_t> counts(stripe_hist, stripe_hist + fanout);
                    string_pieces[s] = ColumnsScatter::scatter<UInt16>(sources, ids, counts, allow_combining);
                    string_pieces[s][extra] = nullptr;
                }

                for (size_t run_begin = stripe_begin[s]; run_begin < stripe_begin[s + 1];)
                {
                    size_t run_end = run_begin;
                    for (size_t rows = 0; run_end < stripe_begin[s + 1] && rows < batch_rows; ++run_end)
                        rows += clause_of(run_end).route.size();

                    /// One column at a time, so the stores of a pass go to one set of pieces.
                    for (size_t b = run_begin; b < run_end; ++b)
                    {
                        auto & clause = clause_of(b);
                        const UInt16 * pids = clause.route.data();
                        const size_t rows = clause.route.size();
                        if constexpr (kind == KeyKind::Number)
                        {
                            const char * keys = clause.key_columns.at(0)->getRawData().data();
                            scatterPidChunk(key_pieces.width, pids, keys, rows, key_pieces.mode, key_scratch);
                        }
                        else if constexpr (kind == KeyKind::Prepared)
                            scatterPidChunk(key_pieces.width, pids, reinterpret_cast<const char *>(clause.key128.data()), rows,
                                            key_pieces.mode, key_scratch);
                        else if constexpr (kind == KeyKind::Packed)
                        {
                            /// Packed again here: packing once in the fill pass would hold 16 to 32 bytes per row while
                            /// the block waits.
                            using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
                            KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(clause.key_columns, table.keySizes());
                            Arena pool;
                            for (size_t first = 0; first < rows; first += chunk_rows)
                            {
                                const size_t n = std::min(chunk_rows, rows - first);
                                for (size_t j = 0; j < n; ++j)
                                {
                                    auto key_holder = key_getter.getKeyHolder(first + j, pool);
                                    packed_chunk[j] = keyHolderGetKey(key_holder);
                                }
                                scatterPidChunk(key_pieces.width, pids + first, reinterpret_cast<const char *>(packed_chunk.data()),
                                                n, key_pieces.mode, key_scratch);
                            }
                        }
                        else
                            scatterPidChunk(hash_pieces.width, pids, reinterpret_cast<const char *>(clause.hash64.data()), rows,
                                            hash_pieces.mode, hash_scratch);
                    }

                    if constexpr (is_asof_join)
                        for (size_t b = run_begin; b < run_end; ++b)
                        {
                            auto & clause = clause_of(b);
                            const char * asof_values = clause.key_columns.back()->getRawData().data();
                            scatterPidChunk(asof_pieces.width, clause.route.data(), asof_values, clause.route.size(), asof_pieces.mode,
                                            asof_scratch);
                        }

                    if (has_locator)
                        for (size_t b = run_begin; b < run_end; ++b)
                        {
                            auto & clause = clause_of(b);
                            const UInt32 block_no = blocks[b]->block_no;
                            const size_t rows = clause.route.size();
                            for (size_t first = 0; first < rows; first += chunk_rows)
                            {
                                const size_t n = std::min(chunk_rows, rows - first);
                                if constexpr (!Maps::has_mapped)
                                    std::fill_n(reinterpret_cast<UInt32 *>(locator_chunk.data()), n, block_no);
                                else if (narrow_locators)
                                {
                                    auto * words = reinterpret_cast<UInt32 *>(locator_chunk.data());
                                    for (size_t j = 0; j < n; ++j)
                                        words[j] = (block_no << 16) | static_cast<UInt32>(first + j);
                                }
                                else
                                {
                                    for (size_t j = 0; j < n; ++j)
                                        locator_chunk[j] = RowRef(block_no, first + j).encode();
                                }
                                scatterPidChunk(locator_pieces.width, clause.route.data() + first,
                                                reinterpret_cast<const char *>(locator_chunk.data()), n, locator_pieces.mode,
                                                locator_scratch);
                            }
                        }

                    /// The pieces now hold what the per-row arrays of the run held.
                    for (size_t b = run_begin; b < run_end; ++b)
                    {
                        auto & clause = clause_of(b);
                        PaddedPODArray<UInt16>().swap(clause.route);
                        PaddedPODArray<UInt8>().swap(clause.skip);
                        PaddedPODArray<UInt128>().swap(clause.key128);
                        PaddedPODArray<UInt64>().swap(clause.hash64);
                    }
                    check_cancelled();
                    run_begin = run_end;
                }

                /// Before the next stripe seeds the cursors again: in the `Combined` mode `seed` restarts the staging line
                /// of a piece, so the rows still staged for this stripe would never reach the piece.
                for (auto [pieces, scratch] : columns)
                    if (!pieces->empty())
                        scratch->drain();
            }
        });
    }

    for (FixedPieces * pieces : all_pieces)
        if (!pieces->empty())
            pieces->release(extra);
    ProfileEvents::increment(ProfileEvents::HashJoinBuildScatterMicroseconds, scatter_watch.elapsedMicroseconds());

    /// The owners: partitions largest first, one owner per partition.
    Stopwatch insert_watch;
    std::vector<size_t> order;
    order.reserve(num_partitions);
    for (size_t p = 0; p < num_partitions; ++p)
        if (piece_rows[p])
            order.push_back(p);
    std::ranges::stable_sort(order, [&](size_t a, size_t b) { return piece_rows[a] > piece_rows[b]; });

    const size_t num_owners
        = std::max<size_t>(1, std::min({settings.max_threads, order.size(), total_rows / MIN_ROWS_PER_OWNER}));
    const size_t l2_share = std::max<size_t>(getL2CacheSize(), 1 << 20) * 4 / 5;
    auto & view = table.template partitionedView<TYPE, Maps>();
    std::atomic<size_t> grown_capacity = view.commonCapacity();
    /// Locals, so that the stores of the insert loop do not make it reload them through `settings`.
    [[maybe_unused]] const bool any_take_last_row = settings.any_take_last_row;
    [[maybe_unused]] const bool drops_blocks = settings.drops_blocks;
    [[maybe_unused]] const TypeIndex asof_type = is_asof_join ? *settings.asof_type : TypeIndex{};
    [[maybe_unused]] const ASOFJoinInequality asof_inequality = settings.asof_inequality;
    [[maybe_unused]] const size_t asof_width = asof_pieces.width;

    std::vector<UInt8> owner_all_values_unique(num_owners, 1);
    {
        std::atomic<size_t> next_partition = 0;
        runWorkers(num_owners, stop, [&](size_t owner)
        {
            /// TODO: size the first chunk of the arena from the predicted arena bytes divided by the owners, when that is at
            /// least 1 MiB. `Arena` has no call for that yet.
            Arena & arena = table.arena(owner);
            bool all_values_unique = true;
            SCOPE_EXIT({ owner_all_values_unique[owner] = all_values_unique; });
            size_t c = 0;
            while (!stop.load(std::memory_order_relaxed) && (c = next_partition.fetch_add(1)) < order.size())
            {
                const size_t p = order[c];
                const size_t rows = piece_rows[p];

                const bool created = view.createPartition(p);
                Map & map = view.partition(p);

                const size_t map_bytes_before = created ? 0 : map.getBufferSizeInBytes();
                const size_t arena_bytes_before = arena.allocatedBytes();
                size_t new_keys = 0;
                SCOPE_EXIT({
                    table.addKeys(new_keys);
                    add_bytes(map.getBufferSizeInBytes() + arena.allocatedBytes() - map_bytes_before - arena_bytes_before);
                });

                const size_t cells_before_insert = map.getBufferSizeInCells();

                const char * keys = fixed_key ? key_pieces.data[p] : nullptr;
                const UInt64 * hashes = fixed_key ? nullptr : reinterpret_cast<const UInt64 *>(hash_pieces.data[p]);
                const char * locators = has_locator ? locator_pieces.data[p] : nullptr;
                const char * asof_values = is_asof_join ? asof_pieces.data[p] : nullptr;
                [[maybe_unused]] size_t hashed_keys = 0;

                const auto key_at = [&](size_t i) __attribute__((always_inline))
                {
                    Key key;
                    memcpy(&key, keys + i * sizeof(Key), sizeof(Key));
                    return key;
                };

                /// A table larger than its share of the L2 exists only when the cap on the partition count stopped the split.
                const bool use_prefetch = settings.enable_software_prefetch && map.getBufferSizeInBytes() > l2_share;
                auto prefetcher = makeJoinPrefetcher(
                    use_prefetch,
                    rows,
                    [&](size_t i) __attribute__((always_inline))
                    {
                        if constexpr (kind == KeyKind::String)
                            map.prefetchByHash(hashes[i]);
                        else
                            map.prefetchByHash(map.hash(key_at(i)));
                    });

                /// The insert loop. The locator width is a template parameter, so that the loop tests it once, not per row.
                const auto insert = [&]<bool narrow>()
                {
                    /// Stores the row of `i` right after the emplace of its key, before the next emplace can move the cells.
                    const auto store = [&](size_t i, typename Map::LookupResult it, bool inserted) __attribute__((always_inline))
                    {
                        new_keys += inserted;
                        if constexpr (Maps::has_mapped)
                        {
                            const Locator loc = readLocator<narrow>(locators, i);
                            if constexpr (std::is_same_v<Maps, HashJoin::MapsOne>)
                            {
                                if (inserted || any_take_last_row)
                                {
                                    new (&it->getMapped()) Mapped(loc.block_no, loc.row);
                                    if (drops_blocks)
                                        state.markReferenced(loc.block_no);
                                }
                            }
                            else if constexpr (std::is_same_v<Maps, HashJoin::MapsAll>)
                            {
                                if (inserted)
                                    new (&it->getMapped()) Mapped(loc.block_no, loc.row);
                                else
                                {
                                    /// The first row of a key is inline; a duplicate moves the rows to a `RowRefList` batch.
                                    it->getMapped().insert(RowRef(loc.block_no, loc.row).encode(), arena);
                                    all_values_unique = false;
                                }
                            }
                            else
                            {
                                static_assert(is_asof_join);
                                Mapped * time_series = &it->getMapped();
                                if (inserted)
                                    time_series = new (time_series) Mapped(createAsofRowRef(asof_type, asof_inequality));
                                (*time_series)->insertValue(asof_values + i * asof_width, loc.block_no, loc.row);
                            }
                        }
                        else if constexpr (narrow)
                        {
                            if (inserted)
                                state.markReferenced(reinterpret_cast<const UInt32 *>(locators)[i]);
                        }
                    };

                    if constexpr (kind == KeyKind::String)
                    {
                        using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
                        /// The owner reads the pieces of the stripes in stripe order, as the fixed columns were written.
                        size_t i = 0;
                        for (size_t s = 0; s < num_stripes; ++s)
                        {
                            const auto & piece = string_pieces[s][p];
                            if (!piece || piece->empty())
                                continue;
                            KeyGetter key_getter(ColumnRawPtrs{piece.get()}, table.keySizes(), nullptr);
                            for (size_t j = 0, n = piece->size(); j < n; ++j, ++i)
                            {
                                prefetcher.prefetchAt(i);
                                typename Map::LookupResult it;
                                bool inserted = false;
                                /// The hash saved by the fill pass.
                                view.emplaceInPartition(map, key_getter.getKeyHolder(j, arena), it, inserted, hashes[i]);
                                store(i, it, inserted);
                                if ((i & 65535) == 65535)
                                    check_cancelled();
                            }
                            string_pieces[s][p] = nullptr;
                        }
                        chassert(i == rows);
                    }
                    else
                    {
                        for (size_t i = 0; i < rows; ++i)
                        {
                            prefetcher.prefetchAt(i);
                            typename Map::LookupResult it;
                            bool inserted = false;
                            const Key key = key_at(i);
                            view.emplaceInPartition(map, key, it, inserted, map.hash(key));
                            if constexpr (TYPE == HashJoin::Type::key_packed_string)
                                hashed_keys += (key.items[1] & PackedStringJoinKey::hashed_flag) != 0;
                            store(i, it, inserted);
                            if ((i & 65535) == 65535)
                                check_cancelled();
                        }
                    }
                };
                if (has_locator && (!Maps::has_mapped || narrow_locators))
                    insert.template operator()<true>();
                else
                    insert.template operator()<false>();

                if constexpr (TYPE == HashJoin::Type::key_packed_string)
                {
                    ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysInline, rows - hashed_keys);
                    ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysHashed, hashed_keys);
                }
                if (const size_t cells = map.getBufferSizeInCells(); cells > cells_before_insert)
                {
                    ProfileEvents::increment(
                        ProfileEvents::HashJoinPartitionTableResizes, std::countr_zero(cells) - std::countr_zero(cells_before_insert));
                    size_t previous = grown_capacity.load(std::memory_order_relaxed);
                    while (previous < cells && !grown_capacity.compare_exchange_weak(previous, cells, std::memory_order_relaxed))
                    {
                    }
                }

                for (FixedPieces * pieces : all_pieces)
                    if (!pieces->empty())
                        pieces->release(p);
                check_cancelled();
            }
        });
    }
    /// Only an exhausted partition can have grown. No owner can still hold a pointer into a map here.
    if (const size_t cells = grown_capacity.load(std::memory_order_relaxed); cells > view.commonCapacity())
        add_bytes(view.ensureCommonCapacity(cells));
    table.publishPartitionCapacity(view.commonCapacity(), view.getBufferSizeInBytes());
    ProfileEvents::increment(ProfileEvents::HashJoinBuildInsertMicroseconds, insert_watch.elapsedMicroseconds());

    for (const UInt8 unique : owner_all_values_unique)
        result.all_values_unique &= unique != 0;

    return result;
}

}

}

namespace DB::JoinPartitionedBuild
{

template <typename Maps>
BuildResult buildRound(HashJoinTable & table, size_t clause_idx, std::span<WaitingBlockRecord * const> blocks,
                       const RoundSettings & settings, HashJoinBuildState & state,
                       absl::FunctionRef<void(size_t)> add_bytes, absl::FunctionRef<void()> check_cancelled)
{
    return table.dispatchType([&]<HashJoin::Type TYPE>() -> BuildResult
    {
        if constexpr (detail::keyKindOf<TYPE>() == detail::KeyKind::None)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key type {} is not built in partitions", static_cast<int>(TYPE));
        else
            return detail::buildRoundTyped<TYPE, Maps>(table, clause_idx, blocks, settings, state, add_bytes, check_cancelled);
    });
}

}
