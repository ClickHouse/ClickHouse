#pragma once
#include <Processors/ISimpleTransform.h>
#include <Columns/FilterDescription.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Interpreters/SetSpillable.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
class QueryConditionCache;

/// All rows of the chunk passed the conditions that the reader applied before the `FilterStep` with `token` evaluates them
/// again, e.g. the conditions that a `FINAL` read applies as PREWHERE to its non-intersecting ranges. `num_rows` is the
/// number of rows of the chunk: a transform that drops rows keeps the property, and `FilterTransform` updates it, while a
/// transform that combines chunks may add rows without it, so the info is valid only while the number matches.
struct PrefilteredChunkInfo : public ChunkInfoCloneable<PrefilteredChunkInfo>
{
    PrefilteredChunkInfo(UInt64 token_, size_t num_rows_) : token(token_), num_rows(num_rows_) {}

    UInt64 token;
    size_t num_rows;
};

/// Adds `PrefilteredChunkInfo` with `token` to every chunk.
class MarkPrefilteredChunksTransform final : public ISimpleTransform
{
public:
    MarkPrefilteredChunksTransform(SharedHeader header_, UInt64 token_)
        : ISimpleTransform(header_, header_, false), token(token_) {}

    String getName() const override { return "MarkPrefilteredChunksTransform"; }

    void transform(Chunk & chunk) override;

private:
    UInt64 token;
};

/** Implements WHERE, HAVING operations.
  * Takes an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions.
  * The expression is evaluated and result chunks contain only the filtered rows.
  * If remove_filter_column is true, remove filter column from block.
  */
class FilterTransform final : public ISimpleTransform
{
public:
    FilterTransform(
        SharedHeader header_, ExpressionActionsPtr expression_, String filter_column_name_,
        bool remove_filter_column_, bool on_totals_ = false, std::shared_ptr<std::atomic<size_t>> rows_filtered_ = nullptr,
        std::optional<std::pair<UInt64, String>> condition_ = std::nullopt,
        bool update_row_numbers_info_ = false);

    /// Use this overload when the transformed header (the header after the expression, but before
    /// removing the filter column) is already known (computed once per step) to avoid recomputing
    /// it in every instance: the computation is linear in the size of the expression's DAG, and a
    /// step creates one transform per stream.
    FilterTransform(
        SharedHeader header_, SharedHeader transformed_header_, ExpressionActionsPtr expression_, String filter_column_name_,
        bool remove_filter_column_, bool on_totals_ = false, std::shared_ptr<std::atomic<size_t>> rows_filtered_ = nullptr,
        std::optional<std::pair<UInt64, String>> condition_ = std::nullopt,
        bool update_row_numbers_info_ = false);

    static Block
    transformHeader(const Block & header, const ActionsDAG * expression, const String & filter_column_name, bool remove_filter_column);

    String getName() const override { return "FilterTransform"; }
    std::span<ISpillable * const> getSpillables() override { return set_spillables.get(); }

    Status prepare() override;

    void transform(Chunk & chunk) override;

    void onCancel() noexcept override;

    static bool canUseType(const DataTypePtr & type);

    /// Evaluate `prefiltered_expression_` instead of the expression for the chunks with `PrefilteredChunkInfo` with
    /// `token`. It must give the same result for them: it treats the conditions they already passed as true.
    void setPrefilteredExpression(ExpressionActionsPtr prefiltered_expression_, UInt64 token);

private:
    ExpressionActionsPtr expression;
    ExpressionActionsPtr prefiltered_expression;
    UInt64 prefiltered_token = 0;
    SetSpillables set_spillables;
    String filter_column_name;
    bool remove_filter_column;
    bool on_totals;
    /// When set, compose the applied filter into the chunk's `ChunkInfoRowNumbers` (if present) so
    /// downstream `_row_number` / positional-delete consumers keep correct physical row numbers.
    /// Opt-in because only callers whose upstream transforms all maintain the info may enable it
    /// (the data lake read filters: every transform above them preserves or updates it).
    bool update_row_numbers_info;
    bool always_false = false;
    size_t filter_column_position = 0;

    std::shared_ptr<std::atomic<size_t>> rows_filtered;

    /// If set, we need to update the query condition cache at runtime for every processed chunk
    std::optional<std::pair<UInt64, String>> condition;

    std::shared_ptr<QueryConditionCache> query_condition_cache;

    MarkRangesInfoPtr buffered_mark_ranges_info; /// Buffers mark info for chunks from the same table and part.
                                                 /// The goal is to write less often into the query condition cache (reduce lock contention).

    /// Header after expression, but before removing filter column.
    Block transformed_header;

    bool are_prepared_sets_initialized = false;

    void doTransform(Chunk & chunk);
    void removeFilterIfNeed(Columns & columns) const;

    void writeIntoQueryConditionCache(const MarkRangesInfoPtr & mark_ranges_info);
    /// For a chunk that is filtered only partially, see `MarkRangesInfo::rows_per_mark`.
    void writeMarksWithoutMatchesIntoQueryConditionCache(const MarkRangesInfoPtr & mark_ranges_info, const IFilterDescription & filter_description);
};

}
