#pragma once

#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

/// A cancellation observed from inside a structure-prefix read. Carries no state: its TYPE is the
/// signal. `name()` is deliberately NOT overridden, so the type stays invisible to clients
/// (`displayText` prepends `name()`, and in-tree tests assert the exact text).
class PrefixReadCancelledException : public Exception
{
public:
    explicit PrefixReadCancelledException(const Exception & cause) : Exception(cause) {}

    PrefixReadCancelledException * clone() const override { return new PrefixReadCancelledException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)
};

/// True when `exception_ptr` holds a `PrefixReadCancelledException`. The one type test for it;
/// `isCancelledPrefixRead` delegates here, because `src/DataTypes` must not depend on
/// `Storages/MergeTree/checkDataPart.h`.
bool isPrefixReadCancelled(std::exception_ptr exception_ptr);

/// Polls query cancellation from inside a structure-prefix read, which the executor cannot interrupt
/// because it only checks between `work()` calls. Throttles on elapsed time, not on an iteration
/// count: path lengths span four orders of magnitude, so "every N iterations" is unbounded.
///
/// Distinct from `DB::CancellationChecker`, the watchdog thread that cancels the `QueryStatus`.
class PrefixReadCancellationChecker
{
public:
    /// Arms this thread's throttle, then polls once, so an already-pending cancellation is honoured at
    /// prefix entry rather than after a grace period. Arming here is what lets the FIRST `check` on a
    /// thread poll; every later construction is a no-op, so the period is not restarted per prefix.
    PrefixReadCancellationChecker()
    {
        static_cast<void>(throttleState());
        throwIfCancelled();
    }

    /// Throws the cancellation cause if the query is cancelled and the period has elapsed. Inert for a
    /// thread group with no process-list element, which is how ordinary merges and part checks run; a
    /// merge started by `OPTIMIZE` does carry one and is cancellable.
    void check()
    {
        auto & throttle = throttleState();
        const UInt64 elapsed = throttle.stopwatch.elapsedMicroseconds();
        if (elapsed < throttle.last_check_time + check_period_microseconds)
            return;

        throttle.last_check_time = elapsed;
        throwIfCancelled();
    }

private:
    /// Rethrows the cancellation cause as a `PrefixReadCancelledException`, preserving `code()` and
    /// `message()`. A cause that is not a `DB::Exception` is propagated untouched.
    static void throwIfCancelled();

    /// When this thread last polled. Thread state, not per-object: one `work()` span reads many
    /// prefixes, so per-object state would restart the period for each and make the bound
    /// O(number of prefixes). A stale timestamp can only make the next poll fire sooner.
    struct ThrottleState
    {
        Stopwatch stopwatch;
        UInt64 last_check_time = 0;
    };

    static ThrottleState & throttleState()
    {
        static thread_local ThrottleState state;
        return state;
    }

    /// Bounds the cancellation delay contributed by prefix reads to ~10 ms of reading, so the
    /// predicate runs at most ~100 times per second regardless of how many paths there are.
    static constexpr UInt64 check_period_microseconds = 10 * 1000;
};

/// Chunked `readStringBinary`: a single path length is capped only by `DEFAULT_MAX_STRING_SIZE`, so
/// both stream-sized halves are chunked - the READ, and the ALLOCATION it fills, since `resize`
/// value-initializes. Rejects the same inputs as `readStringBinary`, with the same error codes.
inline void readPathNameCancellable(String & path, ReadBuffer & buf, PrefixReadCancellationChecker & cancellation_checker)
{
    /// Not measurable for a normal path name, still prompt for a multi-megabyte one.
    static constexpr size_t read_chunk_size = 64 * 1024;

    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    /// Reserve first: growing in steps without it would reallocate and recopy on every step.
    path.clear();
    path.reserve(size);

    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        const size_t bytes_to_read = std::min(read_chunk_size, size - bytes_read);
        path.resize(bytes_read + bytes_to_read);
        const size_t bytes_copied = buf.read(path.data() + bytes_read, bytes_to_read);
        if (bytes_copied != bytes_to_read)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data. Bytes read: {}. Bytes expected: {}.", bytes_read + bytes_copied, size);

        bytes_read += bytes_copied;
        cancellation_checker.check();
    }
}

}
