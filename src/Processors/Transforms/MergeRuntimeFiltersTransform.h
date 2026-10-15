#pragma once
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB
{

/// Largest serialized filter state a receiver accepts. Equal to the streaming exchange packet limit
/// (a `static_assert` in `DistributedPlanExecutor.cpp` checks it); persisted and in-memory exchanges
/// have no limit of their own, so this one applies to them.
constexpr UInt64 MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES = 256ULL * 1024 * 1024;

/// Header of the stream carrying serialized partial runtime filters between tasks.
SharedHeader runtimeFilterPartialsHeader();

/// One-row chunk of `runtimeFilterPartialsHeader` that carries `state`. Counts the state as sent to
/// `num_destinations` streams.
Chunk makeRuntimeFilterPartialsChunk(const String & state, size_t num_destinations);

/// Registers a filter that gets no more merges, so `__applyFilter` can use it at once.
void registerCompleteRuntimeFilter(
    IRuntimeFilterLookup & lookup,
    const String & filter_key,
    const String & filter_name,
    const RuntimeFilterGeometry & geometry,
    AdaptiveSetRuntimeFilter && filter,
    bool enable_index_analysis);

/// Merges one serialized partial runtime filter from each input as it arrives. When every input
/// has delivered, `RegisterUnion` registers the union under `filter_key` and `ForwardUnion` sends it
/// as one row. If an input ends without a state, or a state is larger than
/// `max_received_state_bytes`, nothing is published and rows pass unfiltered. A malformed or
/// duplicate state throws. In `RegisterUnion` mode the output carries no rows.
class MergeRuntimeFiltersTransform final : public IProcessor
{
public:
    enum class Mode
    {
        RegisterUnion,
        ForwardUnion,
    };

    MergeRuntimeFiltersTransform(
        SharedHeader partials_header,
        size_t num_inputs,
        Mode mode_,
        String filter_name_,
        String filter_key_,
        const DataTypePtr & filter_column_target_type_,
        const RuntimeFilterGeometry & geometry_,
        RuntimeFilterLookupPtr filter_lookup_,
        size_t num_forward_destinations_ = 1,
        UInt64 max_received_state_bytes_ = MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES);

    String getName() const override { return "MergeRuntimeFiltersTransform"; }

    Status prepare() override;
    void work() override;

    /// `RegisterUnion` only: let storage index analysis prune granules by the union's exact values.
    void enableIndexAnalysis() { enable_index_analysis = true; }

private:
    void consume();
    void finalize();

    const Mode mode;
    const String filter_name;
    const String filter_key;
    const DataTypePtr filter_column_target_type;
    const RuntimeFilterGeometry geometry;
    RuntimeFilterLookupPtr filter_lookup;
    /// How many destination streams the forwarded union feeds (after the CopyTransform outside);
    /// used only for the sent-state counters.
    const size_t num_forward_destinations;
    const UInt64 max_received_state_bytes;
    bool enable_index_analysis = false;

    /// Which inputs have delivered their state.
    std::vector<bool> received;
    size_t states_received = 0;
    /// Union of the states received so far. Wrapped in a `RuntimeFilter` only when it is registered.
    std::unique_ptr<AdaptiveSetRuntimeFilter> accumulated;
    /// An oversized state was rejected: publish nothing, fail open.
    bool skipped = false;

    Chunk current_chunk;
    size_t current_input = 0;
    bool has_current_chunk = false;
    bool finalized = false;
    Chunk output_chunk;
    bool has_output_chunk = false;
};

}
