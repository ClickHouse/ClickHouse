#pragma once

#include "config.h"

#if USE_SILK

#include <Common/ErrnoException.h>
#include <Common/Exception.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/futex.h>
#include <silk/util/bounded-queue.h>

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PIPE;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
}

namespace Silk
{

/// A bounded channel from a silk fiber to the code that consumes what the fiber produces.
///
/// The producer is a fiber: `push` suspends it while the mailbox is full, so the producer never runs
/// more than `capacity` items ahead of the consumer. The consumer is a plain thread (for example a
/// pipeline thread) or another fiber: `tryPop` neither blocks nor suspends. When it returns nothing,
/// the consumer waits until `getFileDescriptor` becomes readable (for example in `PollingQueue`) and
/// calls `tryPop` again. A wakeup that finds nothing is possible but harmless: it consumes the
/// readiness, so the consumer parks again until the producer pushes something.
///
/// The producer ends the stream with `finish`, optionally with an exception. The consumer receives
/// every item pushed before that, then `tryPop` rethrows the exception once, then `isFinished`
/// becomes true. The consumer ends the stream with `close`: a producer suspended in `push` wakes up,
/// and every further `push` returns false.
///
/// Exactly one producer and one consumer. Items live on the heap, so the size of `T` does not
/// matter; `T` only has to be movable. The owner must `close` the mailbox and wait for the producer
/// fiber to finish before destroying it.
template <typename T>
class FiberMailbox
{
public:
    explicit FiberMailbox(size_t capacity_)
        : capacity(capacity_)
        /// The ring wants a power of two of at least 2; `capacity` itself is enforced with `size`.
        , queue(std::bit_ceil(std::max<size_t>(capacity_, 2)))
    {
        if (capacity == 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The capacity of a fiber mailbox must be positive");

        event_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (event_fd == -1)
            throw DB::ErrnoException(DB::ErrorCodes::CANNOT_PIPE, "Cannot create an eventfd for a fiber mailbox");
    }

    FiberMailbox(const FiberMailbox &) = delete;
    FiberMailbox & operator=(const FiberMailbox &) = delete;

    ~FiberMailbox()
    {
        T * item = nullptr;
        while (queue.dequeue(&item))
            delete item;

        [[maybe_unused]] int res = ::close(event_fd);
        chassert(res == 0);
    }

    /// Producer. Suspends the fiber while the mailbox is full. Returns false once the consumer has
    /// closed the mailbox: the item is dropped and the producer should stop.
    [[nodiscard]] bool push(T value)
    {
        std::unique_ptr<T> item = std::make_unique<T>(std::move(value));

        for (;;)
        {
            if (closed.load(std::memory_order_acquire))
                return false;

            /// The futex protocol: take the token before checking the condition, so that a pop
            /// between the check and the wait advances the counter past the token and the wait
            /// returns at once.
            const uint64_t token = space_available.get();

            /// Counted before it is visible in the ring, so `size` never underestimates the items.
            if (size.fetch_add(1, std::memory_order_acq_rel) < capacity)
            {
                if (queue.enqueue(item.get()))
                {
                    item.release();
                    signal();
                    return true;
                }

                /// The ring is never full here, so this is the spurious failure of the lock-free
                /// queue while the consumer is claiming the very slot we target. Let it finish.
                size.fetch_sub(1, std::memory_order_acq_rel);
                silk::FiberScheduler::yield();
                continue;
            }

            size.fetch_sub(1, std::memory_order_acq_rel);

            /// ECANCELED means `close`; the next iteration observes `closed` and returns.
            [[maybe_unused]] int res = space_available.wait(token + 1);
        }
    }

    /// Producer. No more items will be pushed. If `exception` is set, the consumer rethrows it after
    /// it has taken every item pushed before.
    void finish(std::exception_ptr exception_ = nullptr)
    {
        exception = std::move(exception_);
        finished.store(true, std::memory_order_release);
        signal();
    }

    /// Consumer. Never blocks and never suspends. Rethrows the producer's exception once the items
    /// pushed before it are taken.
    std::optional<T> tryPop()
    {
        drain();

        T * item = nullptr;
        if (queue.dequeue(&item))
        {
            std::unique_ptr<T> holder(item);
            size.fetch_sub(1, std::memory_order_acq_rel);
            space_available.post();
            return std::optional<T>(std::move(*holder));
        }

        /// Reading `exception` is only ordered against the producer after `finished` is observed.
        if (finished.load(std::memory_order_acquire) && exception)
            std::rethrow_exception(std::exchange(exception, nullptr));

        return std::nullopt;
    }

    /// Consumer. True once the producer called `finish` and the consumer has taken every item and
    /// the exception, if any.
    bool isFinished() const
    {
        if (!finished.load(std::memory_order_acquire))
            return false;
        return size.load(std::memory_order_acquire) == 0 && !exception;
    }

    /// Consumer. Wakes a producer suspended in `push` and makes every further `push` return false.
    void close()
    {
        closed.store(true, std::memory_order_release);
        space_available.stop();
    }

    /// Readable when `tryPop` may have something new: an item, the end of the stream, or an exception.
    /// Level-triggered; `tryPop` consumes the readiness.
    int getFileDescriptor() const
    {
        return event_fd;
    }

private:
    void signal() const
    {
        const uint64_t one = 1;
        while (::write(event_fd, &one, sizeof(one)) == -1)
        {
            /// EAGAIN means the counter is about to overflow, which is only possible after
            /// 2^64 - 1 unconsumed signals. The descriptor is readable either way.
            if (errno == EAGAIN)
                return;
            if (errno != EINTR)
                throw DB::ErrnoException(DB::ErrorCodes::CANNOT_WRITE_TO_SOCKET, "Cannot write to the eventfd of a fiber mailbox");
        }
    }

    void drain() const
    {
        uint64_t counter = 0;
        while (::read(event_fd, &counter, sizeof(counter)) == -1)
        {
            if (errno == EAGAIN)
                return;
            if (errno != EINTR)
                throw DB::ErrnoException(DB::ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot read from the eventfd of a fiber mailbox");
        }
    }

    const size_t capacity;
    silk::BoundedQueue<T *> queue;

    /// Items counted by the producer before they enter the ring and by the consumer after they leave it.
    std::atomic<size_t> size{0};
    /// Posted by the consumer after every pop; stopped by `close`.
    silk::FiberFutex space_available;

    std::atomic<bool> finished{false};
    std::atomic<bool> closed{false};
    /// Written by the producer before `finished`, read by the consumer after it.
    std::exception_ptr exception;

    int event_fd = -1;
};

}

#endif
