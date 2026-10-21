#pragma once

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int QUERY_WAS_CANCELLED;
extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
extern const int TIMEOUT_EXCEEDED;
extern const int TOO_SLOW;
}

/// A broken invariant, a memory limit, a deadline or a cancellation: the outer guards of the query. Code that
/// tries an operation to find out whether it is possible must rethrow these instead of reading them as "not
/// possible", because swallowing one would let the query go on instead of aborting (for example, the index
/// analysis would go on to a full scan).
/// The cancellation exception stored by `checkTimeLimit` may carry `QUERY_WAS_CANCELLED_BY_CLIENT`,
/// and an allocation failure below the memory tracker surfaces as `CANNOT_ALLOCATE_MEMORY`.
inline bool isQueryOuterGuardError(int code)
{
    return code == ErrorCodes::LOGICAL_ERROR
        || code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
        || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY
        || code == ErrorCodes::QUERY_WAS_CANCELLED
        || code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT
        || code == ErrorCodes::TIMEOUT_EXCEEDED
        || code == ErrorCodes::TOO_SLOW
        || code == ErrorCodes::ABORTED;
}

}
