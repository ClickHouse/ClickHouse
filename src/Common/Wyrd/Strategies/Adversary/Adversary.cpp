#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Adversary.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <algorithm>
#include <semaphore>
#include <thread>

#if defined(OS_LINUX)
#    include <sched.h>
#endif

namespace DB::Runtime::Wyrd
{

namespace
{

thread_local int lock_free_depth = 0;

#if defined(OS_LINUX)

int chooseCore()
{
    unsigned cores = std::max(1u, std::thread::hardware_concurrency());
    return static_cast<int>(randomNumber() % cores);
}

void pinThisThreadToSingleCore()
{
    static const int core = chooseCore();
    cpu_set_t set{};
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    (void)sched_setaffinity(0, sizeof(set), &set);
}

#else

void pinThisThreadToSingleCore()
{
}

#endif

}

struct Adversary::SuspendedThread
{
    std::binary_semaphore wake{0};
};

Adversary::Adversary(AdversaryParams params_)
    : params(params_)
{
}

void Adversary::reset()
{
    suspend_total.store(0);
    yield_count.store(0);
    spurious_failure_count.store(0);
}

AdversaryResult Adversary::report() const
{
    return AdversaryResult{
        .yields = yield_count.load(),
        .suspends = suspend_total.load(),
        .spurious_failures = spurious_failure_count.load(),
    };
}

void Adversary::threadEnter()
{
    thread_count.fetch_add(1);
    if (params.pin_to_single_core)
        pinThisThreadToSingleCore();
}

void Adversary::threadFault()
{
    if (randomEvent(params.suspend_probability))
        suspendThisThread();
    else
        maybeYield();
}

bool Adversary::threadSpuriousFailure()
{
    if (!randomEvent(params.spurious_failure_probability))
        return false;

    spurious_failure_count.fetch_add(1);
    return true;
}

void Adversary::threadEnterLockFreeScope()
{
    ++lock_free_depth;
}

void Adversary::threadLeaveLockFreeScope()
{
    --lock_free_depth;
}

void Adversary::threadProgress()
{
    if (suspended_count.load() > 0 && randomEvent(params.resume_probability))
        resumeRandomThread();
}

void Adversary::threadExit()
{
    thread_count.fetch_sub(1);
    resumeRandomThread();
}

void Adversary::suspendThisThread()
{
    if (lock_free_depth == 0)
        return;

    auto self = std::make_shared<SuspendedThread>();

    {
        std::lock_guard lock(mutex);

        size_t suspended_now = suspended_count.load();
        if (suspended_now >= params.max_suspended_threads)
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

void Adversary::resumeRandomThread()
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

void Adversary::maybeYield()
{
    if (randomEvent(params.yield_probability))
    {
        std::this_thread::yield();
        yield_count.fetch_add(1);
    }
}

}
