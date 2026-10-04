#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/LockFreeAdversary.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Random.h>
#include <base/defines.h>

#include <fmt/format.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>
#include <utility>
#include <vector>

namespace DB::Runtime::Wyrd
{

namespace
{

thread_local int lock_free_depth = 0;

struct SuspendedThread
{
    std::binary_semaphore wake{0};
};

class LockFreeAdversary : public IAdversary
{
    void suspendThisThread()
    {
        if (lock_free_depth == 0)
            return;

        auto self = std::make_shared<SuspendedThread>();

        {
            std::lock_guard lock(mutex);

            size_t suspended_now = suspended_count.load();
            if (suspended_now >= params.suspended_threads_limit)
                return;
            if (suspended_now + 1 >= thread_count.load())
                return;

            suspended.push_back(self);
            suspended_count.fetch_add(1);
            suspend_total.fetch_add(1);
        }

        self->wake.acquire();
        suspended_count.fetch_sub(1);
    }

    void resumeRandomThread()
    {
        std::shared_ptr<SuspendedThread> victim;

        {
            std::lock_guard lock(mutex);
            if (suspended.empty())
                return;

            size_t index = randomNumber() % suspended.size();
            victim = std::move(suspended[index]);
            std::swap(suspended[index], suspended.back());
            suspended.pop_back();
        }

        victim->wake.release();
    }

    void maybeYield()
    {
        if (randomEventEvery(params.yield_freq))
        {
            std::this_thread::yield();
            yield_count.fetch_add(1);
        }
    }

public:
    explicit LockFreeAdversary(LockFreeAdversaryParams params_)
        : params(params_)
    {
    }

    void reset() override
    {
        suspend_total.store(0);
        yield_count.store(0);
    }

    std::string report() override
    {
        return fmt::format("lockfree: suspends={} yields={}", suspend_total.load(), yield_count.load());
    }

    void threadEnter() override
    {
        thread_count.fetch_add(1);
    }

    void threadFault() override
    {
        if (randomEventEvery(params.suspend_freq))
            suspendThisThread();
        else
            maybeYield();
    }

    bool threadSpuriousFailure() override
    {
        return randomEventEvery(11);
    }

    void threadSetLockFree(bool on) override
    {
        lock_free_depth += on ? 1 : -1;
    }

    void threadProgress() override
    {
        if (suspended_count.load() > 0 && randomEventEvery(params.resume_freq))
            resumeRandomThread();
    }

    void threadExit() override
    {
        thread_count.fetch_sub(1);
        resumeRandomThread();
    }

private:
    const LockFreeAdversaryParams params;

    std::atomic<size_t> thread_count{0};
    std::atomic<size_t> suspended_count{0};
    std::atomic<size_t> suspend_total{0};
    std::atomic<size_t> yield_count{0};

    std::mutex mutex;
    std::vector<std::shared_ptr<SuspendedThread>> suspended TSA_GUARDED_BY(mutex);
};

}

AdversaryPtr createLockFreeAdversary(LockFreeAdversaryParams params)
{
    return std::make_shared<LockFreeAdversary>(params);
}

}
