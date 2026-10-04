#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/YieldAdversary.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Random.h>

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <thread>

#if defined(OS_LINUX)
#    include <sched.h>
#endif

namespace DB::Runtime::Wyrd
{

namespace
{

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

class YieldAdversary : public IAdversary
{
    void maybeYield()
    {
        if (randomEventEvery(params.yield_freq))
        {
            std::this_thread::yield();
            yield_count.fetch_add(1);
        }
    }

public:
    explicit YieldAdversary(YieldAdversaryParams params_)
        : params(params_)
    {
    }

    void reset() override
    {
        yield_count.store(0);
    }

    std::string report() override
    {
        return fmt::format("yield: yields={}", yield_count.load());
    }

    void threadEnter() override
    {
        pinThisThreadToSingleCore();
    }

    void threadFault() override
    {
        maybeYield();
    }

    bool threadSpuriousFailure() override
    {
        return randomEventEvery(11);
    }

    void threadSetLockFree(bool) override
    {
    }

    void threadProgress() override
    {
    }

    void threadExit() override
    {
    }

private:
    const YieldAdversaryParams params;
    std::atomic<size_t> yield_count{0};
};

}

AdversaryPtr createYieldAdversary(YieldAdversaryParams params)
{
    return std::make_shared<YieldAdversary>(params);
}

}
