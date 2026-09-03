#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>

#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>


namespace DB
{

/** Single-producer / single-consumer double buffer coordinator.
  *
  * A background producer thread repeatedly fills one of two logical buffers (indices 0 and 1) by
  * calling a user callback, and hands each filled buffer to the consumer. The consumer takes ready
  * buffers in order and releases each when it is done reading it. With two buffers, the producer can
  * fill one while the consumer processes the other — a depth-1 pipeline (prefetch). At most one
  * buffer is "ready but not yet taken" while the consumer holds the other.
  *
  * The class only coordinates \e which buffer is being filled or consumed; it does not own the
  * buffers themselves. It is used to overlap serialization of the next chunk (producer) with the
  * exchange of the current chunk with an external process (consumer) in the executable-UDF
  * shared-memory transport, but it is deliberately independent of that logic and unit-tested on its
  * own.
  *
  * Synchronization is via a mutex and two condition variables (no busy waiting, no sleeps). The
  * mutex establishes a happens-before relation between the producer's fill of a buffer and the
  * consumer's read of the same buffer, so the payload written into buffer `index` is safely visible
  * to the consumer. Exceptions thrown by the producer callback are captured and rethrown from
  * `next` on the consumer thread, or from `rethrowIfFailed` when the consumer stopped calling `next`.
  * The destructor requests a stop and joins the producer thread.
  */
class DoubleBufferedProducer
{
public:
    struct Item
    {
        size_t index;   /// Which of the two buffers is ready.
        size_t size;    /// Payload size the producer reported for it.
    };

    /// Fills buffer `index`; returns the produced size, or std::nullopt to signal end of input.
    using ProducerFn = std::function<std::optional<size_t>(size_t index)>;

    DoubleBufferedProducer() = default;
    ~DoubleBufferedProducer() { stop(); }

    DoubleBufferedProducer(const DoubleBufferedProducer &) = delete;
    DoubleBufferedProducer & operator=(const DoubleBufferedProducer &) = delete;

    /// Launches the producer thread. `thread_group` (may be null) is inherited so that CPU and
    /// memory of the background work are accounted to the owning query; `producer` runs on it.
    void start(ThreadGroupPtr thread_group, ThreadName thread_name, ProducerFn producer)
    {
        producer_fn = std::move(producer);
        thread = ThreadFromGlobalPool([this, group = std::move(thread_group), thread_name]() mutable
        {
            ThreadGroupSwitcher switcher(std::move(group), thread_name);
            run();
        });
    }

    /// Consumer: blocks until the next buffer is ready; returns std::nullopt once the producer has
    /// finished with no more items. Rethrows a producer exception if one occurred.
    std::optional<Item> next()
    {
        std::unique_lock lock(mutex);
        /// stop_requested must be part of the predicate: stop() wakes this cv and expects the
        /// consumer to unblock even when the producer exited without setting `finished` (e.g. on a
        /// cancellation/teardown path). Otherwise a consumer already blocked here would wait forever.
        consumer_cv.wait(lock, [this] { return ready.has_value() || finished || producer_exception || stop_requested; });

        if (producer_exception && !ready.has_value())
            std::rethrow_exception(producer_exception);

        /// No item left: the producer finished, was stopped, or (handled above) failed.
        if (!ready.has_value())
            return std::nullopt;

        Item item = *ready;
        ready.reset();
        producer_cv.notify_one();
        return item;
    }

    /// Consumer: rethrows the producer exception if one occurred. Meant for a consumer that stops
    /// taking items early (it already got everything it needed) and would otherwise never observe a
    /// producer failure. Call it after `stop`, which joins the producer thread and thus makes its
    /// outcome final.
    void rethrowIfFailed()
    {
        std::lock_guard lock(mutex);
        if (producer_exception)
            std::rethrow_exception(producer_exception);
    }

    /// Consumer: marks buffer `index` free for reuse by the producer.
    void release(size_t index)
    {
        {
            std::lock_guard lock(mutex);
            free_buffers[index] = true;
        }
        producer_cv.notify_one();
    }

    /// Producer: whether the consumer has asked to stop. A callback that can keep working for a
    /// long time should poll this and return `std::nullopt`: `stop` ends the loop between callbacks,
    /// so a callback that has already started is waited for, however long it takes.
    bool isStopRequested() const { return stop_requested.load(std::memory_order_relaxed); }

    /// Requests the producer to stop and joins it. Idempotent; also called by the destructor. Waits
    /// for a callback that is already running - see `isStopRequested`.
    void stop() noexcept
    {
        {
            std::lock_guard lock(mutex);
            stop_requested = true;
        }
        producer_cv.notify_all();
        consumer_cv.notify_all();
        if (thread.joinable())
            thread.join();
    }

private:
    void run()
    {
        try
        {
            while (true)
            {
                size_t index;
                {
                    std::unique_lock lock(mutex);
                    producer_cv.wait(lock, [this] { return stop_requested || (!ready.has_value() && anyFree()); });
                    if (stop_requested)
                        return;
                    index = free_buffers[0] ? 0 : 1;
                    free_buffers[index] = false; /// producer takes it; freed by the consumer via release()
                }

                std::optional<size_t> produced = producer_fn(index);

                std::lock_guard lock(mutex);
                if (!produced.has_value())
                {
                    free_buffers[index] = true; /// nothing was produced into it
                    finished = true;
                    consumer_cv.notify_one();
                    return;
                }
                ready = Item{index, *produced};
                consumer_cv.notify_one();
            }
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            producer_exception = std::current_exception();
            consumer_cv.notify_all();
        }
    }

    bool anyFree() const { return free_buffers[0] || free_buffers[1]; }

    std::mutex mutex;
    std::condition_variable producer_cv;
    std::condition_variable consumer_cv;

    ProducerFn producer_fn;
    std::array<bool, 2> free_buffers{{true, true}};
    std::optional<Item> ready;
    bool finished = false;
    /// Written under the mutex (the condition variables' predicates read it there), but a producer
    /// callback polls it without the lock - see `isStopRequested`.
    std::atomic<bool> stop_requested = false;
    std::exception_ptr producer_exception;

    ThreadFromGlobalPool thread;
};

}
