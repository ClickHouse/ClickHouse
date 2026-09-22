#pragma once

#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

class ColumnLowCardinality;

/// Positions of the non-constant `DISTINCT` key columns in the header (all columns when `columns` is
/// empty). Shared by the `DISTINCT` transforms and `DistinctStep`, which must agree on what the key columns
/// are.
ColumnNumbers calculateDistinctKeyColumnsPositions(const Block & header, const Names & columns);

/// The `LowCardinality` optimization in `DISTINCT` tracks seen dictionary indices in a bitmap and skips
/// hash table insertions for rows whose index was already seen. This helps when many rows share few
/// dictionary entries, but becomes pure overhead when most rows carry a new index (e.g. after a preliminary
/// in-order `DISTINCT` that already removed duplicates).
///
/// This controller observes the first few chunks and measures how many
/// rows reference a dictionary index that was not seen in earlier rows.
/// If nearly all rows do, the bitmap is not filtering anything useful
/// and we disable the optimization.
class LCOptimizationController
{
public:
    bool isEnabled() const { return state != State::Disabled; }

    void update(size_t num_rows, size_t new_indices_in_chunk);

private:
    enum class State : uint8_t
    {
        Observing,
        Enabled,
        Disabled
    };

    /// Number of chunks to observe before deciding.
    static constexpr size_t OBSERVATION_CHUNK_COUNT = 5;

    /// Fraction of rows whose LC dictionary index was seen for the first time.
    /// When this rate is this high, the mask filters almost nothing and its
    /// bookkeeping cost (dictionary hashing, seen-index bitmap, per-row branch)
    /// is not justified.
    static constexpr double NEW_INDEX_RATE_THRESHOLD = 0.95;

    State state = State::Observing;
    size_t chunks_observed = 0;
    size_t rows_observed = 0;
    size_t new_indices_observed = 0;
};

/// The `LowCardinality` fast path of `DISTINCT` (for a single `LowCardinality` key column): tracks which
/// dictionary indices have been seen and builds a mask of the rows that are the first occurrence of their
/// index - only those rows need to be checked for distinctness in the hash table.
class DistinctLowCardinalityFilter
{
public:
    DistinctLowCardinalityFilter();
    ~DistinctLowCardinalityFilter();

    /// If the fast path applies to the column, returns the mask: `mask[i] == 1` for rows that may be new
    /// distinct values. An empty (zero-size) mask means no row of the chunk can be a new value, so the
    /// whole chunk can be skipped. Returns `std::nullopt` when the fast path does not apply (not a
    /// `LowCardinality` column, or the optimization has disabled itself).
    std::optional<IColumn::Filter> buildMaskIfApplicable(const IColumn & column, size_t num_rows);

    /// Estimates additional bitmap storage without creating dictionary state. Each new dictionary needs
    /// one byte per entry. Empty input, reused dictionaries, and an inapplicable optimization need none.
    size_t estimateGrowthMemory(const IColumn & column) const;

    /// The memory occupied by the per-dictionary bitmaps of the seen indices. A bitmap is as large as its
    /// dictionary, whatever the number of rows seen, so it can dominate the memory of a `DISTINCT` over a
    /// few rows of a large dictionary.
    size_t getTotalByteCount() const { return total_byte_count; }

private:
    std::pair<IColumn::Filter, size_t> buildMask(const ColumnLowCardinality & column, size_t num_rows);

    /// Per-dictionary bitmaps of the seen indices (behind a pointer to keep the hashing machinery
    /// types out of this header).
    struct DictionariesState;
    std::unique_ptr<DictionariesState> dictionaries_state;
    size_t total_byte_count = 0;

    LCOptimizationController lc_optimization_controller;
};

/// The comparison keys retained by a `DISTINCT` set and returned by its extractor.
enum class DistinctKeyRepresentation
{
    Columns,
    Hash128,
};

/// Owns hash-based `DISTINCT` state, including `LowCardinality` filtering and size-limit enforcement.
/// `DistinctTransform` uses it for streaming deduplication; `ExternalDistinctTransform` also extracts
/// its retained keys when spilling.
class DistinctSetFilter
{
public:
    /// `skip_null_keys_` drops rows with a `NULL` key component, matching a `Set` filled with
    /// `transform_null_in = 0`. Enable it only for consumers that discard these rows; ordinary
    /// `DISTINCT` treats `NULL` as a value. Remaining keys use the nested, non-nullable columns.
    DistinctSetFilter(
        const Block & header,
        const Names & columns,
        const SizeLimits & set_size_limits_,
        bool skip_null_keys_ = false);

    /// Returns the representation chosen from the materialized input columns. Requires an initialized set.
    DistinctKeyRepresentation getKeyRepresentation() const;

    const ColumnNumbers & getKeyColumnsPositions() const { return key_columns_pos; }
    bool hasKeyColumns() const { return !key_columns_pos.empty(); }

    /// Whether a constant key component is `NULL` (detected only in the `skip_null_keys` mode): every key
    /// then contains a `NULL`, so a consumer that skips `NULL` keys receives nothing at all.
    bool hasConstNullKey() const { return has_const_null_key; }

    /// The number of distinct keys seen so far.
    size_t getTotalRowCount() const;

    /// The memory occupied by the set and by the `LowCardinality` fast path.
    size_t getTotalByteCount() const;

    /// Reads owning comparison-key columns from a frozen set in hash-table iteration order.
    class KeyExtractor
    {
    public:
        virtual ~KeyExtractor() = default;

        /// Returns at most `max_rows` keys, stopping after a complete key reaches `max_bytes` of
        /// allocated column memory. The byte target is soft because a key or an allocation can exceed
        /// it; zero disables it. `max_rows` must be positive. An empty vector marks exhaustion.
        virtual MutableColumns next(size_t max_rows, size_t max_bytes) = 0;
    };

    /// Transfers the hash table, arena, and key metadata into an extractor. With `Columns`, the result
    /// follows `getKeyColumnsPositions`; with `Hash128`, it contains one `UInt128` fingerprint column.
    /// The returned columns own their values independently of the extractor. The table is released
    /// after its final key is materialized, or when the extractor is destroyed early.
    /// Requires at least one retained key and `skip_null_keys_ = false`.
    std::unique_ptr<KeyExtractor> extractKeys() &&;

    /// Normalizes input columns and initializes the set on first use, without inserting keys.
    /// Header constants stay compact because they cannot distinguish rows. The prepared chunk exposes
    /// the materialized memory of the remaining columns needed to estimate filtering copies.
    /// Requires `hasKeyColumns` to be true and `skip_null_keys_ = false`.
    void prepareForInsert(Chunk & chunk);

    /// Estimates peak additional retained memory assuming every row in the prepared chunk is new.
    /// Includes hash-table buffers, string arenas, and `LowCardinality` dictionary bitmaps. Requires an
    /// initialized set and columns normalized by `prepareForInsert`. Saturates at the maximum of `size_t`
    /// when the bound is not representable.
    size_t estimateGrowthMemory(const Chunk & chunk) const;

    /// Estimates filtering workspace for a copy of the prepared input and the top-level row masks.
    /// Excludes retained set growth. Requires columns normalized by `prepareForInsert` and
    /// `skip_null_keys_ = false`. Column-specific temporary allocations can exceed this estimate.
    size_t estimateFilteringMemory(const Chunk & chunk) const;

    /// Inserts unseen keys and retains their first rows, preserving chunk information.
    /// `max_rows_in_distinct` and `max_bytes_in_distinct` apply after insertion. `THROW` raises an
    /// exception when exceeded; `BREAK` retains the crossing chunk and sets `isLimitReached`, so the
    /// caller must stop reading and return the partial result.
    /// In `skip_null_keys` mode, rows with a `NULL` key component are dropped. The result can be empty.
    /// Requires `hasKeyColumns` to be true.
    Chunk filter(Chunk chunk);

    /// Whether a size limit with the 'break' overflow mode was reached: no new key can be added to the
    /// set, so the caller should stop reading and return the partial result.
    bool isLimitReached() const { return limit_reached; }

private:
    ColumnRawPtrs getKeyColumns(const Columns & columns) const;
    void initialize(const ColumnRawPtrs & key_columns);

    const ColumnNumbers key_columns_pos;
    const ColumnNumbers non_constant_columns_pos;
    /// Types of the key columns (following `key_columns_pos`), for the key extraction.
    DataTypes key_types;

    /// Owns the hash table and arena until the filter is destroyed or extraction takes ownership.
    std::unique_ptr<SetVariants> data;
    Sizes key_sizes;
    DistinctLowCardinalityFilter lc_filter;

    /// Restrictions on the maximum size of the set.
    const SizeLimits set_size_limits;
    bool limit_reached = false;

    const bool skip_null_keys;
    bool has_const_null_key = false;
};

}
