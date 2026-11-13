#pragma once

#include <Common/EventFD.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/QueryCoordination.h>
#include <Core/SortDescription.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/** The mutex protects state and one response slot per logical shard; validation and selection
  * run outside it. Each `RemoteQueryExecutor` retains ownership of its connection.
  */
class DistributedTopKCoordinator final
{
public:
    struct Settings
    {
        size_t logical_shards = 0;
        UInt64 limit = 0;
        SortDescription sort_description;
        Block candidate_header;
        std::function<void()> candidate_validation_hook = {};
        std::function<void()> selection_hook = {};
        std::function<void(UInt64)> selection_iteration_hook = {};
    };

    enum class State : uint8_t
    {
        Collecting,
        Selecting,
        FallbackAll,
        Selected,
        Cancelled,
    };

    explicit DistributedTopKCoordinator(Settings settings_);

    bool submit(size_t participant, QueryCoordinationRequest request);

    /// An old worker can return ordinary data without sending a candidate request.
    void markParticipantUnsupported(size_t participant);

    bool hasSubmitted(size_t participant) const;
    int getResponseFileDescriptor(size_t participant) const;
    /// True when `takeResponse` would not wait: the response is set, or the coordination was cancelled and it throws.
    bool isResponseReady(size_t participant) const;

    QueryCoordinationResponse takeResponse(size_t participant);

    void cancel(std::exception_ptr exception = {}) noexcept;
    State getState() const;

private:
    struct Participant
    {
        bool submitted = false;
        bool signalled = false;
        UInt64 request_id = 0;
        Block candidates;
        std::optional<QueryCoordinationResponse> response;
        EventFD event;
    };

    using CandidateBlocks = VectorWithMemoryTracking<Block>;
    /// The inner type matches `QueryCoordinationResponse::selected_ordinals`.
    using SelectedOrdinals = VectorWithMemoryTracking<std::vector<UInt64>>; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    void validateParticipant(size_t participant) const;
    void validateRequest(size_t participant, const QueryCoordinationRequest & request) const;
    SelectedOrdinals select(CandidateBlocks candidates) const;
    void fallbackAllLocked();
    void setResponseLocked(size_t participant, QueryCoordinationResponse response);
    void wakeParticipantLocked(Participant & participant) noexcept;
    void finishBarrierLocked();
    [[noreturn]] void rethrowCancellationLocked() const;

    const Settings settings;
    std::atomic_bool cancellation_requested = false;
    mutable std::mutex mutex;
    std::condition_variable cancellation_condition;
    VectorWithMemoryTracking<std::unique_ptr<Participant>> participants;
    State state = State::Collecting;
    size_t submitted = 0;
    std::optional<Stopwatch> barrier_watch;
    std::exception_ptr cancellation_exception;
};

using DistributedTopKCoordinatorPtr = std::shared_ptr<DistributedTopKCoordinator>;

}
