#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/Arena.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>
#include <base/StringViewHash.h>
#include <string_view>
#include <memory>
#include <vector>

namespace DB
{

/** One post-processed state for one aggregate with TOTALS (and later BY) combinator.
  *
  * After the main aggregation finishes, the Aggregator owns a hash table where
  * each group has a state tuple for every aggregate. For aggregates with a combinator,
  * we need to re-merge those per-group states — into a single global state for TOTALS,
  * or into a state per BY-key for BY.
  *
  * This object owns the post-merged state(s) and knows how to:
  *   - absorb a per-group state from the main hash table,
  *   - finalize a value for a result row.
  *
  * Lifecycle: created once per TOTALS/BY aggregate in Aggregator's constructor.
  * Filled exactly once via absorb() calls (sequentially, from a single thread)
  * before any concurrent finalization starts. After that read-only, thread-safe.
  */
class PostAggregationState
{
public:
    /// Marker of what combinator this post-state serves.
    enum class Kind
    {
        Totals,
        By,
    };

    /// Constructor for TOTALS.
    PostAggregationState(
        size_t aggregate_index_,
        const IAggregateFunction * aggregate_function_,
        size_t state_offset_in_group_);

    /// Constructor for BY.
    /// `by_column_positions_` gives positions (in the overall key_columns array of the query)
    /// of the columns that make up the BY-key for this aggregate.
    PostAggregationState(
        size_t aggregate_index_,
        const IAggregateFunction * aggregate_function_,
        size_t state_offset_in_group_,
        std::vector<size_t> by_column_positions_);

    ~PostAggregationState();

    PostAggregationState(const PostAggregationState &) = delete;
    PostAggregationState & operator=(const PostAggregationState &) = delete;
    PostAggregationState(PostAggregationState &&) = default;
    PostAggregationState & operator=(PostAggregationState &&) = default;

    /// Absorb a per-group state. For TOTALS — mergeed into the single global state.
    /// For BY — merged into the state bucket identified by by-key columns at row_num.
    /// `key_columns` must be the array of key columns for the source hash table pass;
    /// unused for TOTALS.
    void absorb(
        const AggregateDataPtr per_group_place,
        const IColumn ** key_columns,
        size_t row_num,
        Arena * keys_arena);

    /// Finalize into one cell of `target_column` for output row `row_num`.
    /// For TOTALS — the same global value regardless of row_num; the caller still
    /// must insert it for every row. For BY — looked up by by-key at row_num.
    /// `key_columns` is unused for TOTALS; for BY it is the array of output key columns.
    void finalizeInto(
        IColumn & target_column,
        const IColumn ** key_columns,
        size_t row_num,
        Arena * keys_arena);

    size_t getAggregateIndex() const { return aggregate_index; }
    Kind getKind() const { return kind; }

private:
    size_t aggregate_index;
    const IAggregateFunction * aggregate_function;
    size_t state_offset_in_group;
    Kind kind;

    /// Arena for all state memory owned by this post-state.
    std::unique_ptr<Arena> arena;

    /// For TOTALS: the single global state.
    AggregateDataPtr global_state = nullptr;

    /// For BY: positions of BY-columns in the overall key_columns array.
    std::vector<size_t> by_column_positions;

    /// For BY: map from serialized BY-key (StringRef into `arena`) to state.
    using ByStates = HashMapWithSavedHash<std::string_view, AggregateDataPtr, StringViewHash>;    ByStates by_states;

    /// Helper: serialize BY-key from key_columns at row_num into arena, return StringRef.
    std::string_view serializeByKey(const IColumn ** key_columns, size_t row_num, Arena * keys_arena);

    /// Helper: find existing state or create a new one for the given key.
    AggregateDataPtr findOrCreateByState(std::string_view key);
};


/** A collection of PostAggregationState's — one per aggregate with a combinator.
  *
  * Lives inside Aggregator, created once, shared across threads after merge.
  *
  * Filled via computeAll() exactly once on finalization, before any concurrent
  * convertToBlockImpl() calls. After that read-only and thread-safe.
  */
class PostAggregationManager
{
public:
    PostAggregationManager() = default;
    ~PostAggregationManager() = default;

    PostAggregationManager(const PostAggregationManager &) = delete;
    PostAggregationManager & operator=(const PostAggregationManager &) = delete;

    /// Build post-states from aggregate descriptions.
    /// `group_by_key_names` is the list of GROUP BY key names (in order). Used to
    /// resolve BY-column names into positions for BY-aggregates.
    void init(
        const AggregateDescriptions & aggregates,
        const std::vector<const IAggregateFunction *> & aggregate_functions,
        const Sizes & offsets_of_aggregate_states,
        const Names & group_by_key_names);

    /// True if no aggregates have combinators — fast path to skip all work.
    bool empty() const { return states.empty(); }

    bool hasByStates() const;

    /// Call this while iterating over the main aggregation hash table.
    /// Can be invoked many times (e.g. once per two-level bucket). Call markComputed()
    /// when all places have been absorbed.
    /// `key_columns` are the key columns for the same iteration (needed by BY states);
    /// pass nullptr if there are no BY-states.
    template <typename NextFn>
    void computeAll(NextFn && get_next, Arena * keys_arena)
    {
        if (states.empty())
            return;

        /// `get_next` returns a pair: {place, key_columns_row} or {nullptr, *} on end.
        /// If no BY-states are present, key_columns argument is ignored.
        while (true)
        {
            auto [place, key_columns, row_num] = get_next();
            if (!place)
                break;
            for (auto & state : states)
                state->absorb(place, key_columns, row_num, keys_arena);
        }
    }

    /// Mark that computation is complete. Used for assertions / idempotency.
    void markComputed() { computed = true; }

    /// Look up post-state by aggregate index. Returns nullptr if aggregate has no combinator.
    /// O(N) in number of post-states; N is tiny (1-3 per query typically).
    PostAggregationState * find(size_t aggregate_index) const;

    bool isComputed() const { return computed; }

private:
    std::vector<std::unique_ptr<PostAggregationState>> states;
    bool computed = false;
};

}
