#pragma once

#include "config.h"

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/PollSession.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <arrow/flight/types.h>
#include <arrow/table.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>


namespace DB::ArrowFlight
{

using Timestamp = std::chrono::system_clock::time_point;
using Duration = std::chrono::system_clock::duration;

/// We use the ALREADY_EXPIRED timestamp (January 1, 1970) as the expiration time of a ticket or a poll descriptor
/// which is already expired.
inline const Timestamp ALREADY_EXPIRED = Timestamp{Duration{0}};

/// We generate tickets with this prefix.
/// Method DoGet() accepts a ticket which is either 1) a ticket with this prefix; or 2) a SQL query.
/// A valid SQL query can't start with this prefix so method DoGet() can distinguish those cases.
inline const String TICKET_PREFIX = "~TICKET-";

inline bool hasTicketPrefix(const String & ticket)
{
    return ticket.starts_with(TICKET_PREFIX);
}

/// We generate poll descriptors with this prefix.
/// Methods PollFlightInfo() or GetSchema() accept a flight descriptor which is either
/// 1) a normal flight descriptor (a table name or a SQL query); or 2) a poll descriptor with this prefix.
/// A valid SQL query can't start with this prefix so methods PollFlightInfo() and GetSchema() can distinguish those cases.
inline const String POLL_DESCRIPTOR_PREFIX = "~POLL-";

inline bool hasPollDescriptorPrefix(const String & poll_descriptor)
{
    return poll_descriptor.starts_with(POLL_DESCRIPTOR_PREFIX);
}

/// A ticket name with its expiration time.
struct TicketWithExpirationTime
{
    String ticket;
    /// When the ticket expires.
    /// std::nullopt means that the ticket expires after using it in DoGet().
    /// Can be equal to ALREADY_EXPIRED.
    std::optional<Timestamp> expiration_time;
};

/// A poll descriptor's name with its expiration time.
struct PollDescriptorWithExpirationTime
{
    String poll_descriptor;
    /// When the poll descriptor expires.
    /// std::nullopt means that the poll descriptor expires after using it in PollFlightInfo();
    /// Can be equal to ALREADY_EXPIRED.
    std::optional<Timestamp> expiration_time;
};

struct TicketInfo : public TicketWithExpirationTime
{
    std::shared_ptr<arrow::Table> arrow_table;
};

/// Information about a poll descriptor.
/// Objects of type PollDescriptorInfo are stored as a kind of a doubly linked list,
/// the previous object is stored as `previous_info`, and the next object is referenced by `next_poll_descriptor`.
struct PollDescriptorInfo : public PollDescriptorWithExpirationTime
{
    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<const PollDescriptorInfo> previous_info;
    bool evaluating = false;
    bool evaluated = false;

    arrow::flight::FlightDescriptor original_flight_descriptor;
    std::string query_id;

    /// The following fields can be set only if `evaluated == true`:

    /// A success or error error.
    std::optional<arrow::Status> status;

    /// A new ticket. Along with tickets from previous infos (previous_info, previous_info->previous_info, etc.)
    /// represents all tickets associated with this poll descriptor.
    /// Can be unset if there is no block; or it can specify an already expired ticket.
    std::optional<String> ticket;

    /// Adds rows. Along with added rows from previous infos (previous_info, previous_info->previous_info, etc.)
    /// represents the total number of rows associated with this poll descriptor.
    /// Can be unset if there is no rows added.
    std::optional<size_t> rows;

    /// Adds bytes. Along with added bytes from previous infos (previous_info, previous_info->previous_info, etc.)
    /// represents the total number of bytes associated with this poll descriptor.
    /// Can be unset if there is no bytes added.
    std::optional<size_t> bytes;

    /// Next poll descriptor if any.
    /// Can be unset if there is no next poll descriptor (no more blocks are to pull from the query pipeline).
    std::optional<String> next_poll_descriptor;
};

/// Keeps information about calls - e.g. blocks extracted from query pipelines, flight tickets, poll descriptors.
class CallsData
{
public:
    CallsData(std::optional<Duration> tickets_lifetime_, std::optional<Duration> poll_descriptors_lifetime_, LoggerPtr log_);

    /// Creates a flight ticket which allows to download a specified block.
    std::shared_ptr<const TicketInfo> createTicket(std::shared_ptr<arrow::Table> arrow_table);

    [[nodiscard]] arrow::Result<std::shared_ptr<const TicketInfo>> getTicketInfo(const String & ticket) const;

    /// Finds the expiration time for a specified ticket.
    /// If the ticket is not found it means it was expired and removed from the map.
    std::optional<Timestamp> getTicketExpirationTime(const String & ticket) const;

    /// Cancels a ticket to free memory.
    void cancelTicket(const String & ticket);

    void eraseFlightDescriptorMapByQueryId(const String & query_id);
    void eraseFlightDescriptorMapByDescriptor(const String & flight_descriptor);
    void eraseFlightDescriptorMapEntry(const String & flight_descriptor);

    /// Creates a poll descriptor.
    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptor(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info);

    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptor(std::unique_ptr<PollSession> poll_session, const arrow::flight::FlightDescriptor & flight_descriptor, const String & query_id);

    [[nodiscard]] arrow::Result<std::shared_ptr<const PollDescriptorInfo>> getPollDescriptorInfo(const String & poll_descriptor) const;

    /// Finds query id for a specified flight descriptor.
    std::optional<String> getQueryIdByFlightDescriptor(const String & flight_descriptor) const;

    /// Finds the expiration time for a specified poll descriptor.
    PollDescriptorWithExpirationTime getPollDescriptorWithExpirationTime(const String & poll_descriptor) const;

    /// Extends the expiration time of a poll descriptor.
    [[nodiscard]] arrow::Status extendPollDescriptorExpirationTime(const String & poll_descriptor);

    /// Starts evaluation (i.e. getting a block of data) for a specified poll descriptor.
    [[nodiscard]] arrow::Result<std::unique_ptr<PollSession>> startEvaluation(const String & poll_descriptor);

    /// Ends evaluation for a specified poll descriptor.
    void endEvaluation(const String & poll_descriptor, const std::optional<String> & ticket, UInt64 rows, UInt64 bytes, bool last);

    /// Ends evaluation for a specified poll descriptor with an error.
    void endEvaluationWithError(const String & poll_descriptor, const arrow::Status & error_status);

    /// Cancels a poll descriptor to free memory.
    void cancelPollDescriptor(const String & poll_descriptor);

    /// Cancels tickets and poll descriptors if the current time is greater than their expiration time.
    void cancelExpired();

    std::vector<String> collectPollDescriptorsForQueryId(const String & query_id) const;

    /// Waits until maybe it's time to cancel expired tickets or poll descriptors.
    /// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA doesn't support std::unique_lock used with condition_variable.
    void waitNextExpirationTime() const TSA_NO_THREAD_SAFETY_ANALYSIS;

    void stopWaitingNextExpirationTime();

private:
    static String generateTicketName();
    static String generatePollDescriptorName();

    std::optional<Timestamp> calculateTicketExpirationTime(Timestamp current_time) const;
    std::optional<Timestamp> calculatePollDescriptorExpirationTime(Timestamp current_time) const;

    void updateNextExpirationTime() TSA_REQUIRES(mutex);

    void setFlightDescriptorMapLocked(const String & flight_descriptor, const String & query_id) TSA_REQUIRES(mutex);
    void eraseFlightDescriptorMapByQueryIdLocked(const String & query_id) TSA_REQUIRES(mutex);
    void eraseFlightDescriptorMapByDescriptorLocked(const String & flight_descriptor) TSA_REQUIRES(mutex);
    void eraseFlightDescriptorMapEntryLocked(const String & flight_descriptor) TSA_REQUIRES(mutex);
    std::optional<String> getQueryIdByFlightDescriptorLocked(const String & flight_descriptor) const TSA_REQUIRES(mutex);

    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptorImpl(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info, std::optional<arrow::flight::FlightDescriptor> flight_descriptor = std::nullopt, std::optional<String> query_id = std::nullopt);

    static Timestamp now();

    const std::optional<Duration> tickets_lifetime;
    const std::optional<Duration> poll_descriptors_lifetime;
    const LoggerPtr log;
    mutable std::mutex mutex;
    std::unordered_map<String, std::shared_ptr<const TicketInfo>> tickets TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::shared_ptr<const PollDescriptorInfo>> poll_descriptors TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::unique_ptr<PollSession>> poll_sessions TSA_GUARDED_BY(mutex);
    std::condition_variable evaluation_ended;
    /// associates flight descriptors with query id
    std::unordered_map<String, String> flight_descriptor_to_query_id TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::unordered_set<String>> query_id_to_flight_descriptors TSA_GUARDED_BY(mutex);
    /// `tickets_by_expiration_time` and `poll_descriptors_by_expiration_time` are sorted by `expiration_time` so `std::set` is used.
    std::set<std::pair<Timestamp, String>> tickets_by_expiration_time TSA_GUARDED_BY(mutex);
    std::set<std::pair<Timestamp, String>> poll_descriptors_by_expiration_time TSA_GUARDED_BY(mutex);
    std::optional<Timestamp> next_expiration_time TSA_GUARDED_BY(mutex);
    mutable std::condition_variable next_expiration_time_updated;
    bool stop_waiting_next_expiration_time TSA_GUARDED_BY(mutex) = false;
};

}

#endif
