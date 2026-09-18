#pragma once

#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Processors/IAccumulatingTransform.h>

#include <Columns/IColumn_fwd.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

namespace DB
{

/// A tags-side dependency transform for the native PromQL plan.
///
/// The transform deliberately produces no data. It consumes the complete tags
/// stream, publishes the collector only after the input reaches EOF, and keeps
/// the collector failed if execution is cancelled or throws. The samples-side
/// dependency is supplied by QueryPipelineBuilder::addPipelineBefore.
class PromQLNativeTagsBarrierTransform final : public IAccumulatingTransform
{
public:
    using CollectorPtr = std::shared_ptr<ContextTimeSeriesTagsCollector>;
    using TagsVector = VectorWithMemoryTracking<ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr>;

    struct NativeTagsChunk
    {
        ColumnPtr id_column;
        TagsVector tags;
    };

    using TagsExtractor = std::function<NativeTagsChunk(const Chunk &)>;
    using BeforeSealHook = std::function<void()>;
    using AfterSealHook = std::function<void()>;
    using AfterStartHook = std::function<void()>;
    using AfterCancelTransitionHook = std::function<void()>;

    PromQLNativeTagsBarrierTransform(
        SharedHeader input_header,
        CollectorPtr collector_,
        TagsExtractor tags_extractor_,
        BeforeSealHook before_seal_hook_ = {},
        AfterSealHook after_seal_hook_ = {},
        AfterStartHook after_start_hook_ = {},
        AfterCancelTransitionHook after_cancel_transition_hook_ = {});

    ~PromQLNativeTagsBarrierTransform() override;

    String getName() const override { return "PromQLNativeTagsBarrier"; }

    /// IAccumulatingTransform::work is wrapped so every failure leaves the
    /// native dictionary in the terminal failed state.
    void work() override;
    void cancel(CancelReason reason) noexcept override;

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;
    void onCancel() noexcept override;

private:
    enum class LifecycleState : UInt8
    {
        Active,
        Published,
        Cancelled,
    };

    void ensureBuildStarted();
    void cancelBuild() noexcept;

    CollectorPtr collector;
    TagsExtractor tags_extractor;
    BeforeSealHook before_seal_hook;
    AfterSealHook after_seal_hook;
    AfterStartHook after_start_hook;
    AfterCancelTransitionHook after_cancel_transition_hook;

    /// work() is serialized by the executor, but cancellation can run in a
    /// different thread. Keep the lazy-start gate atomic and serialize every
    /// collector mutation with cancellation.
    std::atomic_bool build_started{false};
    /// The Active -> Published CAS is the barrier-success publication point
    /// and competes with Active -> Cancelled in cancel. The collector can be
    /// sealed briefly before this CAS, but addPipelineBefore keeps dependent
    /// readers closed until the barrier itself completes successfully.
    std::atomic<LifecycleState> lifecycle_state{LifecycleState::Active};
    std::mutex collector_mutex;
};

}
