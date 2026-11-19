#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulatedThread.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Common/Exception.h>
#include <base/defines.h>

#if defined(ADDRESS_SANITIZER)
#    include <sanitizer/lsan_interface.h>
#endif

#include <fmt/format.h>

#include <atomic>
#include <optional>
#include <tuple>

static std::atomic<bool> leaked_unfinished_threads = false;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Runtime::Wyrd
{

struct TraceRecord
{
    UInt32 thread_id;
    std::string event;
    std::optional<std::source_location> location;
};

namespace
{

thread_local Simulation * current_simulation = nullptr;

std::string formatTimeline(const VectorWithMemoryTracking<TraceRecord> & timeline)
{
    std::string out;
    for (size_t i = 0; i < timeline.size(); ++i)
    {
        out += fmt::format("#{} thread {}: {}", i + 1, timeline[i].thread_id, timeline[i].event);
        if (timeline[i].location)
            out += fmt::format(" at {}:{}", timeline[i].location->file_name(), timeline[i].location->line());
        out += '\n';
    }
    return out;
}

}

Simulation::Simulation(SimulationParams params_)
    : params(params_)
    , scheduler(params)
{
}

Simulation::~Simulation() = default;

SimulationResult Simulation::run(std::function<void()> main)
{
    if (current_simulation)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested simulation");

    setRandomSeed(params.seed);
    createThread(std::move(main));
    SimulationResult::Status status = runLoop();
    leakUnfinishedThreads();

    return SimulationResult{
        .status = status,
        .exception_message = exception ? getExceptionMessage(exception, false) : broken_invariant,
        .total_steps = steps,
        .digest = digest.get64(),
        .timeline = formatTimeline(timeline),
    };
}

SimulationResult::Status Simulation::runLoop()
{
    while (true)
    {
        SimulatedThread * next = scheduler.pickNext();
        if (!next)
            return alive == 0 ? SimulationResult::Status::Ok : SimulationResult::Status::Deadlock;

        if (steps >= params.max_steps)
            return SimulationResult::Status::StepLimit;

        ++steps;

        running_thread = next;
        current_simulation = this;
        next->resume();
        current_simulation = nullptr;
        running_thread = nullptr;

        if (!broken_invariant.empty())
            return SimulationResult::Status::BrokenInvariant;

        if (next->finished())
        {
            if (exception)
                return SimulationResult::Status::Exception;
            --alive;
            scheduler.onExit();
        }
    }
}

void Simulation::traceEvent(std::string event, std::source_location location)
{
    digest.update(running_thread->id);
    digest.update(location.line());

    if (params.collect_timeline)
        timeline.push_back(TraceRecord{.thread_id = running_thread->id, .event = std::move(event), .location = location});
}

void Simulation::createThread(std::function<void()> user_body)
{
    auto guarded_body = [this, body = std::move(user_body)]
    {
        try
        {
            body();
        }
        catch (const boost::context::detail::forced_unwind &)
        {
            throw;
        }
        catch (...)
        {
            exception = std::current_exception();
        }
    };

    threads.push_back(std::make_unique<SimulatedThread>(next_thread_id++, params.fiber_stack_size, std::move(guarded_body)));
    ++alive;
    scheduler.onRunnable(threads.back().get());
}

void Simulation::leakUnfinishedThreads()
{
    for (auto & thread : threads)
    {
        if (thread->finished())
            continue;

        std::ignore = thread.release();
        leaked_unfinished_threads.store(true);
    }
}

Simulation * Simulation::current()
{
    return current_simulation;
}

void Simulation::sync(const char * operation, std::source_location location)
{
    traceEvent(operation, location);
    if (scheduler.shouldHold())
    {
        traceEvent("held", location);
        scheduler.onHold();
    }
    else
    {
        scheduler.onSync();
    }
    running_thread->suspend();
}

void Simulation::yield(std::source_location location)
{
    traceEvent("yield", location);
    scheduler.onYield();
    running_thread->suspend();
}

void Simulation::futexWait(void * address, std::source_location location)
{
    traceEvent("futexWait", location);
    if (scheduler.shouldWakeSpuriously())
    {
        traceEvent("spuriousWakeup", location);
        scheduler.onYield();
    }
    else
    {
        futexes.park(address, running_thread);
        scheduler.onPark();
    }
    running_thread->suspend();
}

void Simulation::futexWake(void * address, size_t count, std::source_location location)
{
    traceEvent("futexWake", location);
    for (SimulatedThread * waiter : futexes.wake(address, count))
        scheduler.onRunnable(waiter);
}

void Simulation::spawn(std::function<void()> body, std::source_location location)
{
    traceEvent(fmt::format("spawn thread {}", next_thread_id), location);
    createThread(std::move(body));
}

bool Simulation::spuriousFailure(std::source_location location)
{
    traceEvent("spuriousFailure", location);
    return randomEvent(params.spurious_failure_probability);
}

void Simulation::enterLockFreeScope(std::source_location location)
{
    traceEvent("enterLockFreeScope", location);
    scheduler.onLockFreeEnter();
}

void Simulation::leaveLockFreeScope(std::source_location location)
{
    traceEvent("leaveLockFreeScope", location);
    scheduler.onLockFreeLeave();
}

void Simulation::progressHint(std::source_location location)
{
    traceEvent("progressHint", location);
    scheduler.onProgress();
}

void Simulation::trace(std::string message)
{
    if (params.collect_timeline)
        timeline.push_back(TraceRecord{.thread_id = running_thread->id, .event = std::move(message), .location = std::nullopt});
}

void Simulation::brokenInvariant(std::string message)
{
    broken_invariant = std::move(message);
    running_thread->suspend();
}

}

}

#if defined(ADDRESS_SANITIZER)
extern "C" int __lsan_is_turned_off()
{
    return leaked_unfinished_threads.load() ? 1 : 0;
}
#endif
