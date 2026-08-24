#pragma once
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB
{

/// Hard safety cap on one serialized runtime filter state accepted by a receiver. Kept equal to
/// `StreamingExchangeProtocol::MAX_DATA_PACKET_BODY_BYTES` (the limit the streaming data plane
/// already enforces on both ends of every exchange packet) so a filter state is never more
/// permissive than any other exchange payload; the equality is pinned by a static_assert in
/// `DistributedPlanExecutor.cpp`. A compliant sender stays far below this bound (the geometry caps
/// a state at `MAX_RUNTIME_BLOOM_FILTER_BYTES`), so only a buggy or hostile peer can hit it; the
/// check exists because the persisted (temporary file) and in-memory exchange paths have no
/// packet-level limit of their own.
constexpr UInt64 MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES = 256ULL * 1024 * 1024;

/// Header of the stream carrying serialized partial runtime filters between tasks.
SharedHeader runtimeFilterPartialsHeader();

/// Collects serialized partial runtime filter states, one per input, and merges each state into a
/// private accumulator immediately as it arrives, so the retained memory is one accumulated filter
/// plus per-input delivery flags -- never the serialized payloads themselves. When every input has
/// delivered, the union is published:
///   - `Mode::RegisterUnion`: registered in the per-query filter map under the given key, so that
///     `__applyFilter` in this task starts pruning.
///   - `Mode::ForwardUnion`: re-serialized and emitted as a single row on the output, for an
///     intermediate task of the runtime filter merge tree.
/// If some input finishes without delivering (e.g. the stream was cancelled), or any state exceeds
/// `max_received_state_bytes`, nothing is published and rows keep passing unfiltered (fail-open).
/// An oversized state is rejected before it is copied or parsed. Malformed or duplicate states
/// still throw: they indicate a bug, not a benign delivery failure.
/// In `RegisterUnion` mode the output never produces rows; it exists so the receiving branch can
/// end in its own sink. The pipeline executor seeds scheduling from sinks, so ending the branch in
/// its own sink makes the sources run eagerly. That is a correctness requirement, not an
/// optimization: on a remote worker the data sinks stay idle until the join pulls the probe side,
/// which waits for the build stage to finish, which waits for these very sources to connect and
/// take the filter -- a branch folded into the data streams deadlocks the whole plan.
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

    /// Compact per-source delivery metadata: which inputs delivered their state already.
    std::vector<bool> received;
    size_t states_received = 0;
    /// The single private accumulated filter; arrived states are merged into it and destroyed.
    std::unique_ptr<ApproximateRuntimeFilter> accumulated;
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
