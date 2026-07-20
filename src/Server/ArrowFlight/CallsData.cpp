#include <Server/ArrowFlight/CallsData.h>

#if USE_ARROWFLIGHT

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace ArrowFlight
{

Timestamp CallsData::now()
{
    return std::chrono::system_clock::now();
}

CallsData::CallsData(std::optional<Duration> tickets_lifetime_, std::optional<Duration> poll_descriptors_lifetime_, LoggerPtr log_)
    : tickets_lifetime(tickets_lifetime_)
    , poll_descriptors_lifetime(poll_descriptors_lifetime_)
    , log(log_)
{
}

std::shared_ptr<const TicketInfo> CallsData::createTicket(std::shared_ptr<arrow::Table> arrow_table)
{
    String ticket = generateTicketName();
    LOG_DEBUG(log, "Creating ticket {}", ticket);
    auto expiration_time = calculateTicketExpirationTime(now());
    auto info = std::make_shared<TicketInfo>();
    info->ticket = ticket;
    info->expiration_time = expiration_time;
    info->arrow_table = arrow_table;
    std::lock_guard lock{mutex};
    bool inserted = tickets.try_emplace(ticket, info).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(inserted); /// Flight tickets are unique.
    if (expiration_time)
    {
        inserted = tickets_by_expiration_time.emplace(*expiration_time, ticket).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted); /// Flight tickets are unique.
        updateNextExpirationTime();
    }
    return info;
}

arrow::Result<std::shared_ptr<const TicketInfo>> CallsData::getTicketInfo(const String & ticket) const
{
    std::lock_guard lock{mutex};
    auto it = tickets.find(ticket);
    if (it == tickets.end())
        return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
    return it->second;
}

std::optional<Timestamp> CallsData::getTicketExpirationTime(const String & ticket) const
{
    if (!tickets_lifetime)
        return std::nullopt;
    std::lock_guard lock{mutex};
    auto it = tickets.find(ticket);
    if (it == tickets.end())
        return ALREADY_EXPIRED;
    return it->second->expiration_time;
}

void CallsData::cancelTicket(const String & ticket)
{
    std::lock_guard lock{mutex};
    auto it = tickets.find(ticket);
    if (it == tickets.end())
        return; /// The ticket has been already cancelled.
    LOG_DEBUG(log, "Cancelling ticket {}", ticket);
    auto info = it->second;
    tickets.erase(it);
    if (info->expiration_time)
    {
        tickets_by_expiration_time.erase(std::make_pair(*info->expiration_time, ticket));
        updateNextExpirationTime();
    }
}

void CallsData::setFlightDescriptorMapLocked(const String & flight_descriptor, const String & query_id)
{
    flight_descriptor_to_query_id[flight_descriptor] = query_id;
    query_id_to_flight_descriptors[query_id].insert(flight_descriptor);
}

void CallsData::eraseFlightDescriptorMapByQueryIdLocked(const String & query_id)
{
    auto it = query_id_to_flight_descriptors.find(query_id);
    if (it == query_id_to_flight_descriptors.end())
        return;
    for (const auto & flight_descriptor : it->second)
        flight_descriptor_to_query_id.erase(flight_descriptor);
    query_id_to_flight_descriptors.erase(it);
}

void CallsData::eraseFlightDescriptorMapByQueryId(const String & query_id)
{
    std::lock_guard lock{mutex};
    eraseFlightDescriptorMapByQueryIdLocked(query_id);
}

void CallsData::eraseFlightDescriptorMapByDescriptorLocked(const String & flight_descriptor)
{
    if (!flight_descriptor_to_query_id.contains(flight_descriptor))
        return;
    eraseFlightDescriptorMapByQueryIdLocked(flight_descriptor_to_query_id[flight_descriptor]);
}

void CallsData::eraseFlightDescriptorMapByDescriptor(const String & flight_descriptor)
{
    std::lock_guard lock{mutex};
    eraseFlightDescriptorMapByDescriptorLocked(flight_descriptor);
}

void CallsData::eraseFlightDescriptorMapEntryLocked(const String & flight_descriptor)
{
    auto it_fd = flight_descriptor_to_query_id.find(flight_descriptor);
    if (it_fd == flight_descriptor_to_query_id.end())
        return;

    String query_id = it_fd->second;
    flight_descriptor_to_query_id.erase(it_fd);

    auto it_q = query_id_to_flight_descriptors.find(query_id);
    if (it_q == query_id_to_flight_descriptors.end())
        return;

    it_q->second.erase(flight_descriptor);
    if (it_q->second.empty())
        query_id_to_flight_descriptors.erase(it_q);
}

void CallsData::eraseFlightDescriptorMapEntry(const String & flight_descriptor)
{
    std::lock_guard lock{mutex};
    eraseFlightDescriptorMapEntryLocked(flight_descriptor);
}

std::shared_ptr<const PollDescriptorInfo>
CallsData::createPollDescriptorImpl(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info, std::optional<arrow::flight::FlightDescriptor> flight_descriptor, std::optional<String> query_id)
{
    String poll_descriptor;
    std::lock_guard lock{mutex};
    if (previous_info)
    {
        if (!previous_info->evaluated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is not evaluated");
        if (!previous_info->next_poll_descriptor)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is final");
        poll_descriptor = *previous_info->next_poll_descriptor;
        query_id = getQueryIdByFlightDescriptorLocked(previous_info->poll_descriptor);
        if (!query_id)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                "Cannot create continuation poll descriptor: previous poll descriptor {} was expired or cancelled",
                previous_info->poll_descriptor);
    }
    else
    {
        poll_descriptor = generatePollDescriptorName();
    }
    LOG_DEBUG(log, "Creating poll descriptor {}", poll_descriptor);
    auto current_time = now();
    auto expiration_time = calculatePollDescriptorExpirationTime(current_time);
    auto info = std::make_shared<PollDescriptorInfo>();
    info->poll_descriptor = poll_descriptor;
    info->expiration_time = expiration_time;
    info->schema = poll_session->getSchema();
    info->previous_info = previous_info;
    info->query_id = *query_id;
    if (previous_info)
        info->original_flight_descriptor = previous_info->original_flight_descriptor;
    else
        info->original_flight_descriptor = *flight_descriptor;
    bool inserted = poll_descriptors.try_emplace(poll_descriptor, info).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(inserted); /// Poll descriptors are unique.
    inserted = poll_sessions.try_emplace(poll_descriptor, std::move(poll_session)).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(inserted); /// Poll descriptors are unique.
    if (expiration_time)
    {
        inserted = poll_descriptors_by_expiration_time.emplace(*expiration_time, poll_descriptor).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted); /// Poll descriptors are unique.
        updateNextExpirationTime();
    }
    if (query_id)
        setFlightDescriptorMapLocked(poll_descriptor, *query_id);
    return info;
}

std::shared_ptr<const PollDescriptorInfo>
CallsData::createPollDescriptor(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info)
{
    return createPollDescriptorImpl(std::move(poll_session), previous_info);
}

std::shared_ptr<const PollDescriptorInfo>
CallsData::createPollDescriptor(std::unique_ptr<PollSession> poll_session, const arrow::flight::FlightDescriptor & flight_descriptor, const String & query_id)
{
    return createPollDescriptorImpl(std::move(poll_session), nullptr, flight_descriptor, query_id);
}

arrow::Result<std::shared_ptr<const PollDescriptorInfo>> CallsData::getPollDescriptorInfo(const String & poll_descriptor) const
{
    std::lock_guard lock{mutex};
    auto it = poll_descriptors.find(poll_descriptor);
    if (it == poll_descriptors.end())
        return arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
    return it->second;
}

std::optional<String> CallsData::getQueryIdByFlightDescriptorLocked(const String & flight_descriptor) const
{
    auto it = flight_descriptor_to_query_id.find(flight_descriptor);
    if (it == flight_descriptor_to_query_id.end())
        return std::nullopt;
    return it->second;
}

std::optional<String> CallsData::getQueryIdByFlightDescriptor(const String & flight_descriptor) const
{
    std::lock_guard lock{mutex};
    return getQueryIdByFlightDescriptorLocked(flight_descriptor);
}

PollDescriptorWithExpirationTime CallsData::getPollDescriptorWithExpirationTime(const String & poll_descriptor) const
{
    if (!poll_descriptors_lifetime)
        return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = std::nullopt};
    std::lock_guard lock{mutex};
    auto it = poll_descriptors.find(poll_descriptor);
    if (it == poll_descriptors.end())
        return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = ALREADY_EXPIRED};
    return *it->second;
}

arrow::Status CallsData::extendPollDescriptorExpirationTime(const String & poll_descriptor)
{
    if (!poll_descriptors_lifetime)
        return arrow::Status::OK();
    auto current_time = now();
    std::lock_guard lock{mutex};
    auto it = poll_descriptors.find(poll_descriptor);
    if (it == poll_descriptors.end())
        return arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
    auto info = it->second;
    auto old_expiration_time = info->expiration_time;
    auto new_expiration_time = calculatePollDescriptorExpirationTime(current_time);
    auto new_info = std::make_shared<PollDescriptorInfo>(*info);
    new_info->expiration_time = new_expiration_time;
    it->second = new_info;
    poll_descriptors_by_expiration_time.erase(std::make_pair(*old_expiration_time, poll_descriptor));
    poll_descriptors_by_expiration_time.emplace(*new_expiration_time, poll_descriptor);
    updateNextExpirationTime();
    return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<PollSession>> CallsData::startEvaluation(const String & poll_descriptor)
{
    arrow::Result<std::unique_ptr<PollSession>> res;
    std::unique_lock lock{mutex};
    evaluation_ended.wait(lock, [&]() TSA_REQUIRES(mutex)
    {
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
        {
            res = arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
            return true;
        }
        auto info = it->second;
        if (info->evaluated)
        {
            res = std::unique_ptr<PollSession>{nullptr};
            return true;
        }
        if (!info->evaluating)
        {
            auto it2 = poll_sessions.find(poll_descriptor);
            if (it2 == poll_sessions.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Session is not attached to non-evaluated poll descriptor {}", poll_descriptor);
            res = std::move(it2->second);
            poll_sessions.erase(it2);
            auto new_info = std::make_shared<PollDescriptorInfo>(*info);
            new_info->evaluating = true;
            it->second = new_info;
            if (info->expiration_time)
            {
                poll_descriptors_by_expiration_time.erase(std::make_pair(*info->expiration_time, poll_descriptor));
                updateNextExpirationTime();
            }
            return true;
        }
        return false; /// The poll descriptor is being evaluated in another thread, we need to wait.
    });
    return res;
}

void CallsData::endEvaluation(const String & poll_descriptor, const std::optional<String> & ticket, UInt64 rows, UInt64 bytes, bool last)
{
    std::lock_guard lock{mutex};
    auto it = poll_descriptors.find(poll_descriptor);
    if (it == poll_descriptors.end())
    {
        /// The poll descriptor expired during the query execution.
        evaluation_ended.notify_all();
        return;
    }

    auto info = it->second;
    if (info->evaluated)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Poll descriptor can't be evaluated twice");

    auto new_info = std::make_shared<PollDescriptorInfo>(*info);
    new_info->evaluating = false;
    new_info->evaluated = true;
    new_info->status = arrow::Status::OK();
    new_info->ticket = ticket;
    new_info->rows = rows;
    new_info->bytes = bytes;
    if (!last)
        new_info->next_poll_descriptor = generatePollDescriptorName();
    auto new_expiration_time = calculatePollDescriptorExpirationTime(now());
    new_info->expiration_time = new_expiration_time;
    it->second = new_info;
    if (new_expiration_time)
    {
        poll_descriptors_by_expiration_time.emplace(*new_expiration_time, poll_descriptor);
        updateNextExpirationTime();
    }
    info = new_info;
    evaluation_ended.notify_all();
}

void CallsData::endEvaluationWithError(const String & poll_descriptor, const arrow::Status & error_status)
{
    chassert(!error_status.ok());
    std::lock_guard lock{mutex};
    auto it = poll_descriptors.find(poll_descriptor);
    if (it != poll_descriptors.end())
    {
        auto info = it->second;
        if (!info->evaluated)
        {
            auto new_info = std::make_shared<PollDescriptorInfo>(*info);
            new_info->evaluating = false;
            new_info->evaluated = true;
            new_info->status = error_status;
            auto new_expiration_time = calculatePollDescriptorExpirationTime(now());
            new_info->expiration_time = new_expiration_time;
            it->second = new_info;
            if (new_expiration_time)
            {
                poll_descriptors_by_expiration_time.emplace(*new_expiration_time, poll_descriptor);
                updateNextExpirationTime();
            }
            info = new_info;
            evaluation_ended.notify_all();
        }
    }
    else
    {
        evaluation_ended.notify_all();
    }
}

void CallsData::cancelPollDescriptor(const String & poll_descriptor)
{
    std::unique_ptr<PollSession> poll_session_to_cancel;
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it != poll_descriptors.end())
        {
            LOG_DEBUG(log, "Cancelling poll descriptor {}", poll_descriptor);
            auto info = it->second;
            poll_descriptors.erase(it);
            if (info->expiration_time)
            {
                poll_descriptors_by_expiration_time.erase(std::make_pair(*info->expiration_time, poll_descriptor));
                updateNextExpirationTime();
            }
        }
        auto it2 = poll_sessions.find(poll_descriptor);
        if (it2 != poll_sessions.end())
        {
            poll_session_to_cancel = std::move(it2->second);
            poll_sessions.erase(it2);
        }
        eraseFlightDescriptorMapEntryLocked(poll_descriptor);
        evaluation_ended.notify_all();
    }

    if (poll_session_to_cancel)
    {
        try
        {
            poll_session_to_cancel->onCancelOrConnectionLoss();
        }
        catch (...)
        {
            tryLogCurrentException(log, "cancelPollDescriptor: block_io.onCancelOrConnectionLoss failed");
        }
    }
}

void CallsData::cancelExpired()
{
    std::vector<std::unique_ptr<PollSession>> poll_sessions_to_cancel;
    auto current_time = now();
    {
        std::lock_guard lock{mutex};
        while (!tickets_by_expiration_time.empty())
        {
            auto it = tickets_by_expiration_time.begin();
            if (current_time <= it->first)
                break;
            LOG_DEBUG(log, "Cancelling expired ticket {}", it->second);
            tickets.erase(it->second);
            tickets_by_expiration_time.erase(it);
        }

        for (auto it = poll_descriptors_by_expiration_time.begin(); it != poll_descriptors_by_expiration_time.end();)
        {
            if (current_time <= it->first)
                break;

            auto pd_it = poll_descriptors.find(it->second);
            chassert(pd_it != poll_descriptors.end());
            if (pd_it == poll_descriptors.end())
            {
                LOG_WARNING(log, "Poll descriptor {} found in expiration index but not in poll_descriptors; removing stale expiration entry", it->second);
                it = poll_descriptors_by_expiration_time.erase(it);
                continue;
            }

            chassert(!pd_it->second->evaluating);
            if (pd_it->second->evaluating)
            {
                ++it;
                continue;
            }

            LOG_DEBUG(log, "Cancelling expired poll descriptor {}", it->second);
            poll_descriptors.erase(pd_it);
            auto it2 = poll_sessions.find(it->second);
            if (it2 != poll_sessions.end())
            {
                poll_sessions_to_cancel.emplace_back(std::move(it2->second));
                poll_sessions.erase(it2);
            }
            eraseFlightDescriptorMapEntryLocked(it->second);
            it = poll_descriptors_by_expiration_time.erase(it);
        }
        updateNextExpirationTime();
    }

    for (auto & session : poll_sessions_to_cancel)
    {
        if (!session)
            continue;

        try
        {
            session->onCancelOrConnectionLoss();
        }
        catch (...)
        {
            tryLogCurrentException(log, "cancelExpired: block_io.onCancelOrConnectionLoss failed");
        }
    }
}

std::vector<String> CallsData::collectPollDescriptorsForQueryId(const String & query_id) const
{
    std::lock_guard lock{mutex};
    auto it = query_id_to_flight_descriptors.find(query_id);
    if (it == query_id_to_flight_descriptors.end())
        return {};
    return {it->second.begin(), it->second.end()};
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA doesn't support std::unique_lock used with condition_variable.
void CallsData::waitNextExpirationTime() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    auto current_time = now();
    std::unique_lock lock{mutex};
    auto expiration_time = next_expiration_time;
    /// TSA_NO_THREAD_SAFETY_ANALYSIS because the mutex is held by the enclosing unique_lock, but TSA can't see that inside a lambda.
    auto is_ready = [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        if (stop_waiting_next_expiration_time)
            return true;
        if (next_expiration_time != expiration_time)
            return true; /// We need to restart waiting if the next expiration time has changed.
        current_time = now();
        return (expiration_time && (current_time > *expiration_time));
    };
    if (expiration_time)
    {
        if (current_time < *expiration_time)
            next_expiration_time_updated.wait_for(lock, *expiration_time - current_time, is_ready);
    }
    else
    {
        next_expiration_time_updated.wait(lock, is_ready);
    }
}

void CallsData::stopWaitingNextExpirationTime()
{
    std::lock_guard lock{mutex};
    stop_waiting_next_expiration_time = true;
    next_expiration_time_updated.notify_all();
}

String CallsData::generateTicketName()
{
    return TICKET_PREFIX + toString(UUIDHelpers::generateV4());
}

String CallsData::generatePollDescriptorName()
{
    return POLL_DESCRIPTOR_PREFIX + toString(UUIDHelpers::generateV4());
}

std::optional<Timestamp> CallsData::calculateTicketExpirationTime(Timestamp current_time) const
{
    if (!tickets_lifetime)
        return std::nullopt;
    return current_time + *tickets_lifetime;
}

std::optional<Timestamp> CallsData::calculatePollDescriptorExpirationTime(Timestamp current_time) const
{
    if (!poll_descriptors_lifetime)
        return std::nullopt;
    return current_time + *poll_descriptors_lifetime;
}

void CallsData::updateNextExpirationTime()
{
    auto expiration_time = next_expiration_time;
    next_expiration_time.reset();
    if (!tickets_by_expiration_time.empty())
        next_expiration_time = tickets_by_expiration_time.begin()->first;
    if (!poll_descriptors_by_expiration_time.empty())
    {
        auto other_expiration_time = poll_descriptors_by_expiration_time.begin()->first;
        next_expiration_time = next_expiration_time ? std::min(*next_expiration_time, other_expiration_time) : other_expiration_time;
    }
    if (next_expiration_time != expiration_time)
        next_expiration_time_updated.notify_all();
}

}
}

#endif
