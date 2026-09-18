#include <Processors/Transforms/PromQLNativeTagsBarrierTransform.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

PromQLNativeTagsBarrierTransform::PromQLNativeTagsBarrierTransform(
    SharedHeader input_header,
    CollectorPtr collector_,
    TagsExtractor tags_extractor_,
    BeforeSealHook before_seal_hook_,
    AfterSealHook after_seal_hook_,
    AfterStartHook after_start_hook_,
    AfterCancelTransitionHook after_cancel_transition_hook_)
    : IAccumulatingTransform(std::move(input_header), std::make_shared<const Block>(Block{}))
    , collector(std::move(collector_))
    , tags_extractor(std::move(tags_extractor_))
    , before_seal_hook(std::move(before_seal_hook_))
    , after_seal_hook(std::move(after_seal_hook_))
    , after_start_hook(std::move(after_start_hook_))
    , after_cancel_transition_hook(std::move(after_cancel_transition_hook_))
{
    if (!collector)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier requires a tags collector");
    if (!tags_extractor)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier requires a tags extractor");
}

PromQLNativeTagsBarrierTransform::~PromQLNativeTagsBarrierTransform()
{
    /// A pipeline can be built and abandoned before its first work call. The
    /// collector API makes this safe after Disabled, Sealed, or Failed too.
    collector->abortNativeSeriesDictionaryBuild();
}

void PromQLNativeTagsBarrierTransform::ensureBuildStarted()
{
    if (build_started.load(std::memory_order_acquire))
        return;

    bool cancelled_while_starting = false;
    {
        std::lock_guard lock{collector_mutex};
        if (build_started.load(std::memory_order_relaxed))
            return;

        /// Do not transition a merely constructed pipeline to Building. This also
        /// closes the cancel-before-start race: the second check below prevents a
        /// cancellation observed by cancel from being followed by a live build.
        if ((lifecycle_state.load(std::memory_order_acquire) != LifecycleState::Active) || isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled before starting");

        collector->startNativeSeriesDictionaryBuild();
        build_started.store(true, std::memory_order_release);

        if (after_start_hook)
            after_start_hook();

        cancelled_while_starting
            = (lifecycle_state.load(std::memory_order_acquire) != LifecycleState::Active) || isCancelled();
    }

    if (cancelled_while_starting)
    {
        cancelBuild();
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled while starting");
    }
}

void PromQLNativeTagsBarrierTransform::cancelBuild() noexcept
{
    auto expected = LifecycleState::Active;
    const bool transitioned = lifecycle_state.compare_exchange_strong(
        expected, LifecycleState::Cancelled, std::memory_order_acq_rel, std::memory_order_acquire);
    if (transitioned && after_cancel_transition_hook)
        after_cancel_transition_hook();

    if (transitioned || (expected == LifecycleState::Cancelled))
    {
        /// Calling this more than once is intentional: if another cancellation
        /// thread won the state transition but was descheduled before reaching
        /// the collector, this thread still completes the revocation. The
        /// mutex makes every collector mutation linearize before or after the
        /// cancellation transition.
        std::lock_guard lock{collector_mutex};
        collector->cancelNativeSeriesDictionaryBuild();
    }
}

void PromQLNativeTagsBarrierTransform::consume(Chunk chunk)
{
    auto extracted = tags_extractor(chunk);
    if (!extracted.id_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native tags barrier extractor returned no identifier column");

    if (extracted.id_column->size() != extracted.tags.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native tags barrier extractor returned {} identifiers and {} tag rows",
            extracted.id_column->size(), extracted.tags.size());

    std::lock_guard lock{collector_mutex};
    if ((lifecycle_state.load(std::memory_order_acquire) != LifecycleState::Active) || isCancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled while consuming tags");

    collector->storeTagsForNativeSeriesDictionary(extracted.id_column, extracted.tags);
}

Chunk PromQLNativeTagsBarrierTransform::generate()
{
    if (before_seal_hook)
        before_seal_hook();

    {
        std::lock_guard lock{collector_mutex};
        if ((lifecycle_state.load(std::memory_order_acquire) != LifecycleState::Active) || isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled before sealing");

        collector->finishNativeSeriesDictionaryBuild();
    }

    if (after_seal_hook)
        after_seal_hook();

    if (isCancelled())
    {
        cancelBuild();
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled while sealing");
    }

    /// This CAS is the barrier-success publication point. If cancel wins the race,
    /// the dictionary is revoked and the tags branch cannot complete. If this
    /// CAS wins, later cancellation is executor teardown (or a concurrent
    /// cancellation linearized after successful publication) and must not
    /// revoke the dictionary before the dependent samples branch observes it.
    auto expected = LifecycleState::Active;
    if (!lifecycle_state.compare_exchange_strong(
            expected, LifecycleState::Published, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        cancelBuild();
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PromQL native tags barrier was cancelled before publication");
    }

    return {};
}

void PromQLNativeTagsBarrierTransform::cancel(CancelReason reason) noexcept
{
    if (reason != CancelReason::PartialResult)
        cancelBuild();

    IAccumulatingTransform::cancel(reason);
}

void PromQLNativeTagsBarrierTransform::work()
{
    try
    {
        ensureBuildStarted();
        IAccumulatingTransform::work();
    }
    catch (...)
    {
        cancelBuild();
        throw;
    }
}

void PromQLNativeTagsBarrierTransform::onCancel() noexcept
{
    cancelBuild();
}

}
