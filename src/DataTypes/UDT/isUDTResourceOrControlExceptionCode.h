#pragma once

namespace DB::ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int CANNOT_SCHEDULE_TASK;
extern const int DEADLOCK_AVOIDED;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int QUERY_WAS_CANCELLED;
extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
extern const int TIMEOUT_EXCEEDED;
}

namespace DB::UDT
{

/// Exceptions in this set describe transient process/resource control rather
/// than malformed durable UDT bytes. Broad validation/recovery catches must
/// preserve them so startup can be retried instead of recording false durable
/// damage. ABORTED is intentionally excluded: UDT boundaries also use it for
/// deterministic identity/integrity rejection and classify that case locally.
inline bool isUDTResourceOrControlExceptionCode(int code) noexcept
{
    return code == ErrorCodes::CANNOT_ALLOCATE_MEMORY || code == ErrorCodes::CANNOT_SCHEDULE_TASK || code == ErrorCodes::DEADLOCK_AVOIDED
        || code == ErrorCodes::MEMORY_LIMIT_EXCEEDED || code == ErrorCodes::QUERY_WAS_CANCELLED
        || code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT || code == ErrorCodes::TIMEOUT_EXCEEDED;
}

}
