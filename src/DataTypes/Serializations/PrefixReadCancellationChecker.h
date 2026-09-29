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
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};

/// True when `exception_ptr` holds a `PrefixReadCancelledException`. The one type test for it;
/// `isCancelledPrefixRead` delegates here, because `src/DataTypes` must not depend on
/// `Storages/MergeTree/checkDataPart.h`.
bool isPrefixReadCancelled(std::exception_ptr exception_ptr);

/// Polls query cancellation from inside a structure-prefix read, which the executor cannot interrupt
/// because it only checks between `work()` calls. Throttles on elapsed time, not on an iteration
/// count: name lengths span four orders of magnitude, so "every N iterations" is unbounded.
///
/// Distinct from `DB::CancellationChecker`, the watchdog thread that cancels the `QueryStatus`.
class PrefixReadCancellationChecker
{
public:
    /// Touching the throttle starts this thread's clock here rather than at the first `check`, and leaves
    /// the elapsed period alone, so a later prefix does not restart it.
    PrefixReadCancellationChecker()
        : throttle(throttleState())
    {
    }

    /// Throws the cancellation cause if the query is cancelled and the period has elapsed. Inert for a
    /// thread group with no process-list element, which is how background merges and part checks run.
    /// `processed_bytes` is the work done since the previous call, such as one name's length.
    void check(size_t processed_bytes = clock_read_bytes)
    {
        throttle.unclocked_bytes += processed_bytes + min_call_bytes;
        if (throttle.unclocked_bytes < clock_read_bytes)
            return;

        throttle.unclocked_bytes = 0;
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

    /// When this thread last polled, and how much work it did since it last read the clock. Thread
    /// state, not per-object: one `work()` span reads many prefixes, so per-object state would restart
    /// the period for each and make the bound O(number of prefixes). A stale timestamp can only make the
    /// next poll fire sooner.
    struct ThrottleState
    {
        Stopwatch stopwatch;
        UInt64 last_check_time = 0;
        size_t unclocked_bytes = 0;
    };

    /// Defined out of line so there is one state per thread, not one per shared library: a mutable
    /// `thread_local` in an inline function is a distinct object in every library that uses it.
    static ThrottleState & throttleState();

    /// Bounds the cancellation delay contributed by prefix reads to ~10 ms of reading, so the
    /// predicate runs at most ~100 times per second regardless of how many paths there are.
    static constexpr UInt64 check_period_microseconds = 10 * 1000;

    /// The clock is read at least every 4 KiB of work and every 256 calls.
    static constexpr size_t clock_read_bytes = 4096;
    static constexpr size_t min_call_bytes = 16;

    ThrottleState & throttle;
};

/// Chunked `readStringBinary`: the READ is chunked, and so is the value-initialization `resize` performs
/// on each step's bytes. The buffer itself is allocated once, for the declared length, exactly as
/// `readStringBinary` does. Same accepted inputs, same error codes.
inline void readStringBinaryCancellable(String & s, ReadBuffer & buf, PrefixReadCancellationChecker & cancellation_checker)
{
    /// Not measurable for a normal name, still prompt for a multi-megabyte one.
    static constexpr size_t read_chunk_size = 64 * 1024;

    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    /// One chunk: read it whole, as `readStringBinary` does.
    if (size <= read_chunk_size)
    {
        s.resize(size);
        buf.readStrict(s.data(), size);
        return;
    }

    /// One allocation for the declared length: growing in steps instead relocates the whole buffer each
    /// time capacity doubles, and a relocation near the end copies half the name uninterruptibly.
    s.clear();
    s.reserve(size);

    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        const size_t bytes_to_read = std::min(read_chunk_size, size - bytes_read);
        s.resize(bytes_read + bytes_to_read);
        const size_t bytes_copied = buf.read(s.data() + bytes_read, bytes_to_read);
        if (bytes_copied != bytes_to_read)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data. Bytes read: {}. Bytes expected: {}.", bytes_read + bytes_copied, size);

        bytes_read += bytes_copied;
        /// Not after the last chunk: every caller polls once per name it reads.
        if (bytes_read < size)
            cancellation_checker.check();
    }
}

}
