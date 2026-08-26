#pragma once

#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

class ColumnLowCardinality;

/// Positions of the non-constant DISTINCT key columns in the header (all columns when `columns` is
/// empty). Shared by the DISTINCT transforms and DistinctStep, which must agree on what the key
/// columns are.
ColumnNumbers calculateDistinctKeyColumnsPositions(const Block & header, const Names & columns);

/// The LowCardinality optimization in DISTINCT tracks seen dictionary indices
/// in a bitmap and skips hash table insertions for rows whose index was
/// already seen. This helps when many rows share few dictionary entries, but
/// becomes pure overhead when most rows carry a new index (e.g. after a
/// preliminary in-order DISTINCT that already removed duplicates).
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

/// The LowCardinality fast path of DISTINCT (for a single LowCardinality key column): tracks which
/// dictionary indices have been seen and builds a mask of the rows that are the first occurrence of
/// their index - only those rows need to be checked for distinctness in the hash table.
class DistinctLowCardinalityFilter
{
public:
    DistinctLowCardinalityFilter();
    ~DistinctLowCardinalityFilter();

    /// If the fast path applies to the column, returns the mask: mask[i] == 1 for rows that may be new
    /// distinct values. An empty (zero-size) mask means no row of the chunk can be a new value, so the
    /// whole chunk can be skipped. Returns std::nullopt when the fast path does not apply (not a
    /// LowCardinality column, or the optimization has disabled itself).
    std::optional<IColumn::Filter> buildMaskIfApplicable(const IColumn & column, size_t num_rows);

    /// Frees the accumulated per-dictionary state.
    void clear();

private:
    std::pair<IColumn::Filter, size_t> buildMask(const ColumnLowCardinality & column, size_t num_rows);

    /// Per-dictionary bitmaps of the seen indices (behind a pointer to keep the hashing machinery
    /// types out of this header).
    struct DictionariesState;
    std::unique_ptr<DictionariesState> dictionaries_state;

    LCOptimizationController lc_optimization_controller;
};

/// The hash-based DISTINCT semantics in one place: a set of the seen keys, the chunk filtering around
/// it (including the LowCardinality fast path), and the enforcement of the DISTINCT size limits.
/// Owned by DistinctTransform and ExternalDistinctTransform, so that a semantic change to DISTINCT is
/// a change to this class only, and both transforms only add their specific behavior around it
/// (streaming and the pass-through mode in the former, spilling to disk in the latter).
class DistinctSetFilter
{
public:
    DistinctSetFilter(const Block & header, const Names & columns, const SizeLimits & set_size_limits_);

    const ColumnNumbers & getKeyColumnsPositions() const { return key_columns_pos; }
    bool hasKeyColumns() const { return !key_columns_pos.empty(); }

    /// The number of distinct keys seen so far.
    size_t getTotalRowCount() const;

    /// The memory occupied by the set.
    size_t getTotalByteCount() const;

    /// Whether the keys can be materialized back into columns from the set itself. True for every set
    /// method except `hashed`, which keeps only a 128-bit hash per key (chosen for multi-column keys
    /// with variable-width or LowCardinality types). Meaningful once at least one chunk was filtered
    /// (the method is chosen by the first one).
    bool supportsKeyExtraction() const;

    /// Materializes all the keys of the set into columns, in batches of at most max_batch_rows. The
    /// columns of each batch follow getKeyColumnsPositions(): types are those of the corresponding
    /// header columns, in the same order. The keys are returned in the iteration order of the hash
    /// table (no particular order).
    std::vector<MutableColumns> extractKeyColumns(size_t max_batch_rows) const;

    /// Filters the chunk leaving only the rows whose key was not seen before (and inserts their keys
    /// into the set). This is also the enforcement point of the DISTINCT size limits
    /// (max_rows_in_distinct, max_bytes_in_distinct): with the 'throw' overflow mode an exception is
    /// thrown here; with 'break' the new rows of the chunk that crosses a limit are still returned
    /// (their keys are in the set) and isLimitReached starts returning true - the caller should stop
    /// reading, returning the partial result. The limits are enforced here deliberately, so that all
    /// the users of this class share one semantics.
    /// The result may have no rows (nothing new in the chunk). Chunk infos are preserved.
    Chunk filter(Chunk chunk);

    /// Whether a size limit with the 'break' overflow mode was reached: no new key can be added to the
    /// set, so the caller should stop reading and return the partial result.
    bool isLimitReached() const { return limit_reached; }

    /// Frees the set and the LowCardinality state. The filter must not be used afterwards.
    void clear();

private:
    const ColumnNumbers key_columns_pos;
    /// Types of the key columns (following key_columns_pos), for the key extraction.
    DataTypes key_types;

    /// Behind a pointer so that it can be freed by clear() (SetVariants is not movable).
    std::unique_ptr<SetVariants> data;
    Sizes key_sizes;
    DistinctLowCardinalityFilter lc_filter;

    /// Restrictions on the maximum size of the set.
    const SizeLimits set_size_limits;
    bool limit_reached = false;
};

}
