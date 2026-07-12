#include <QueryPipeline/DistributedTopKCoordinator.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Core/SortCursor.h>
#include <Interpreters/sortBlock.h>

#include <unordered_set>
#include <utility>

namespace ProfileEvents
{
    extern const Event DistributedTopKBarrierMicroseconds;
    extern const Event DistributedTopKCandidateBytes;
    extern const Event DistributedTopKCandidateRows;
    extern const Event DistributedTopKFallbacks;
    extern const Event DistributedTopKFallbackShards;
    extern const Event DistributedTopKSelectedRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

DistributedTopKCoordinator::DistributedTopKCoordinator(Settings settings_)
    : settings(std::move(settings_))
{
    if (settings.logical_shards == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed Top-K coordination requires at least one logical shard");
    if (settings.limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed Top-K coordination requires a positive limit");
    if (settings.sort_description.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed Top-K coordination requires a sort description");

    std::unordered_set<String> expected_keys;
    for (const auto & key : settings.sort_description)
    {
        if (expected_keys.emplace(key.column_name).second && !settings.candidate_header.has(key.column_name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed Top-K candidate header is missing sort key {}",
                key.column_name);
    }
    if (settings.candidate_header.columns() != expected_keys.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed Top-K candidate header does not match the sort keys");

    participants.reserve(settings.logical_shards);
    for (size_t i = 0; i < settings.logical_shards; ++i)
        participants.emplace_back(std::make_unique<Participant>());
}

void DistributedTopKCoordinator::validateParticipant(size_t participant) const
{
    if (participant >= participants.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed Top-K participant {} is outside the logical shard set of size {}",
            participant,
            participants.size());
}

void DistributedTopKCoordinator::validateRequest(size_t participant, const QueryCoordinationRequest & request) const
{
    if (request.kind != QueryCoordinationRequestKind::DistributedTopKCandidates)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical shard {} submitted an unexpected coordination request kind", participant);
    if (request.version != QueryCoordinationRequest::CURRENT_VERSION)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Logical shard {} submitted unsupported query coordination version {}",
            participant,
            request.version);
    if (static_cast<UInt64>(request.mode) > static_cast<UInt64>(QueryCoordinationRequestMode::MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Logical shard {} submitted an unknown coordination request mode", participant);
    if (request.mode == QueryCoordinationRequestMode::FallbackAll)
    {
        if (request.payload.columns() != 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "FallbackAll query coordination request from logical shard {} contains a payload",
                participant);
        return;
    }
    if (request.payload.columns() != settings.candidate_header.columns())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Distributed Top-K candidate block from logical shard {} has {} columns, expected {}",
            participant,
            request.payload.columns(),
            settings.candidate_header.columns());

    std::unordered_set<String> actual_keys;
    actual_keys.reserve(request.payload.columns());
    for (const auto & column : request.payload)
    {
        if (!actual_keys.emplace(column.name).second || !settings.candidate_header.has(column.name))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Distributed Top-K candidate block from logical shard {} contains unexpected or duplicate column {}",
                participant,
                column.name);

        const auto & expected = settings.candidate_header.getByName(column.name);
        if (!expected.type->equals(*column.type))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Distributed Top-K sort key {} has type {}, expected {}",
                column.name,
                column.type->getName(),
                expected.type->getName());
    }

    const auto check_cancellation = [this]
    {
        if (cancellation_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K candidate validation was cancelled");
        if (settings.candidate_validation_hook)
            settings.candidate_validation_hook();
        if (cancellation_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K candidate validation was cancelled");
    };
    if (!isAlreadySorted(request.payload, settings.sort_description, check_cancellation))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Distributed Top-K candidate block from logical shard {} is not sorted", participant);
}

bool DistributedTopKCoordinator::submit(size_t participant, QueryCoordinationRequest request)
{
    validateParticipant(participant);
    {
        std::lock_guard lock(mutex);
        if (state == State::Cancelled)
            rethrowCancellationLocked();
        if (participants[participant]->submitted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical shard {} submitted distributed Top-K candidates more than once", participant);
    }

    validateRequest(participant, request);
    const bool fallback_requested = request.mode == QueryCoordinationRequestMode::FallbackAll;
    const size_t rows = fallback_requested ? 0 : request.payload.rows();
    const size_t bytes = fallback_requested ? 0 : request.payload.allocatedBytes();

    CandidateBlocks candidates;
    std::unique_lock lock(mutex);
    if (state == State::Cancelled)
        rethrowCancellationLocked();

    auto & entry = *participants[participant];
    if (entry.submitted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical shard {} submitted distributed Top-K candidates more than once", participant);

    if (state == State::Collecting && !barrier_watch)
        barrier_watch.emplace(CLOCK_MONOTONIC_COARSE);

    entry.submitted = true;
    entry.request_id = request.request_id;
    ++submitted;

    if (fallback_requested)
    {
        fallbackAllLocked();
        setResponseLocked(
            participant,
            QueryCoordinationResponse{
                .request_id = request.request_id, .mode = QueryCoordinationResponseMode::FallbackAll, .selected_ordinals = {}});
        return true;
    }

    ProfileEvents::increment(ProfileEvents::DistributedTopKCandidateRows, rows);
    ProfileEvents::increment(ProfileEvents::DistributedTopKCandidateBytes, bytes);

    if (state == State::FallbackAll)
    {
        setResponseLocked(
            participant,
            QueryCoordinationResponse{
                .request_id = request.request_id, .mode = QueryCoordinationResponseMode::FallbackAll, .selected_ordinals = {}});
        return true;
    }

    if (rows > settings.limit)
    {
        fallbackAllLocked();
        setResponseLocked(
            participant,
            QueryCoordinationResponse{
                .request_id = request.request_id, .mode = QueryCoordinationResponseMode::FallbackAll, .selected_ordinals = {}});
        return true;
    }

    entry.candidates = std::move(request.payload);

    if (submitted != participants.size())
        return false;

    candidates.reserve(participants.size());
    for (auto & other : participants)
        candidates.emplace_back(std::move(other->candidates));
    state = State::Selecting;
    lock.unlock();

    SelectedOrdinals selected;
    try
    {
        if (settings.selection_hook)
            settings.selection_hook();
        selected = select(std::move(candidates));
    }
    catch (...)
    {
        auto selection_exception = std::current_exception();
        if (!cancellation_requested.load(std::memory_order_acquire))
            cancel(std::move(selection_exception));

        std::unique_lock cancellation_lock(mutex);
        cancellation_condition.wait(cancellation_lock, [this] { return state == State::Cancelled; });
        rethrowCancellationLocked();
    }

    UInt64 selected_rows = 0;
    for (const auto & ordinals : selected)
        selected_rows += ordinals.size();

    lock.lock();
    if (cancellation_requested.load(std::memory_order_acquire))
    {
        cancellation_condition.wait(lock, [this] { return state == State::Cancelled; });
        rethrowCancellationLocked();
    }
    if (state != State::Selecting)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed Top-K selection completed in an unexpected coordinator state");

    ProfileEvents::increment(ProfileEvents::DistributedTopKSelectedRows, selected_rows);
    finishBarrierLocked();
    state = State::Selected;
    for (size_t index = 0; index < participants.size(); ++index)
    {
        setResponseLocked(
            index,
            QueryCoordinationResponse{
                .request_id = participants[index]->request_id,
                .mode = QueryCoordinationResponseMode::Selected,
                .selected_ordinals = std::move(selected[index])});
    }
    return true;
}

void DistributedTopKCoordinator::markParticipantUnsupported(size_t participant)
{
    std::lock_guard lock(mutex);
    validateParticipant(participant);

    if (state == State::Cancelled)
        return;

    if (!participants[participant]->submitted && state == State::Collecting)
        fallbackAllLocked();
}

bool DistributedTopKCoordinator::hasSubmitted(size_t participant) const
{
    std::lock_guard lock(mutex);
    validateParticipant(participant);
    return participants[participant]->submitted;
}

int DistributedTopKCoordinator::getResponseFileDescriptor(size_t participant) const
{
    std::lock_guard lock(mutex);
    validateParticipant(participant);
#if defined(OS_LINUX) || defined(OS_DARWIN)
    return participants[participant]->event.fd;
#else
    return -1;
#endif
}

QueryCoordinationResponse DistributedTopKCoordinator::takeResponse(size_t participant)
{
    std::unique_lock lock(mutex);
    validateParticipant(participant);
    if (state == State::Cancelled)
        rethrowCancellationLocked();

    auto & entry = *participants[participant];
    if (!entry.response || !entry.signalled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed Top-K response for logical shard {} is not ready", participant);

    auto response = std::move(*entry.response);
    entry.response.reset();
    lock.unlock();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    entry.event.read();
#endif

    lock.lock();
    entry.signalled = false;
    if (state == State::Cancelled)
        rethrowCancellationLocked();
    return response;
}

void DistributedTopKCoordinator::cancel(std::exception_ptr exception) noexcept
{
    try
    {
        std::lock_guard lock(mutex);
        if (state == State::Cancelled)
            return;

        cancellation_requested.store(true, std::memory_order_release);
        state = State::Cancelled;
        cancellation_exception = std::move(exception);
        for (auto & participant : participants)
            wakeParticipantLocked(*participant);
    }
    catch (...)
    {
    }
    cancellation_condition.notify_all();
}

DistributedTopKCoordinator::State DistributedTopKCoordinator::getState() const
{
    std::lock_guard lock(mutex);
    return state;
}

DistributedTopKCoordinator::SelectedOrdinals DistributedTopKCoordinator::select(CandidateBlocks candidates) const
{
    if (cancellation_requested.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K selection was cancelled");

    SortCursorImpls cursors;
    cursors.reserve(candidates.size());
    for (size_t participant = 0; participant < candidates.size(); ++participant)
        cursors.emplace_back(candidates[participant], settings.sort_description, participant);

    SortingQueue<SortCursorWithCollation> queue(cursors);
    SelectedOrdinals selected(candidates.size());
    UInt64 remaining = settings.limit;
    while (remaining != 0 && queue.isValid())
    {
        if (cancellation_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K selection was cancelled");
        if (settings.selection_iteration_hook)
            settings.selection_iteration_hook(settings.limit - remaining);
        if (cancellation_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K selection was cancelled");

        auto & cursor = queue.current();
        selected[cursor->order].push_back(cursor->getRow());
        --remaining;
        queue.next();
    }

    return selected;
}

void DistributedTopKCoordinator::fallbackAllLocked()
{
    if (state != State::Collecting)
        return;

    ProfileEvents::increment(ProfileEvents::DistributedTopKFallbacks);
    ProfileEvents::increment(ProfileEvents::DistributedTopKFallbackShards, participants.size());
    finishBarrierLocked();
    state = State::FallbackAll;
    for (size_t participant = 0; participant < participants.size(); ++participant)
    {
        const auto & entry = *participants[participant];
        if (!entry.submitted || entry.response)
            continue;

        setResponseLocked(
            participant,
            QueryCoordinationResponse{
                .request_id = entry.request_id, .mode = QueryCoordinationResponseMode::FallbackAll, .selected_ordinals = {}});
        participants[participant]->candidates.clear();
    }
}

void DistributedTopKCoordinator::setResponseLocked(size_t participant, QueryCoordinationResponse response)
{
    auto & entry = *participants[participant];
    if (entry.response)
        return;

    entry.response = std::move(response);
    wakeParticipantLocked(entry);
}

void DistributedTopKCoordinator::wakeParticipantLocked(Participant & participant) noexcept
{
    if (participant.signalled)
        return;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    participant.event.write();
#endif
    participant.signalled = true;
}

void DistributedTopKCoordinator::finishBarrierLocked()
{
    if (!barrier_watch)
        return;

    ProfileEvents::increment(ProfileEvents::DistributedTopKBarrierMicroseconds, barrier_watch->elapsedMicroseconds());
    barrier_watch.reset();
}

void DistributedTopKCoordinator::rethrowCancellationLocked() const
{
    if (cancellation_exception)
        std::rethrow_exception(cancellation_exception);
    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed Top-K coordination was cancelled");
}

}
