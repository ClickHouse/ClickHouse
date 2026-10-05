#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/SimulatedThread.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Common/Exception.h>

#include <fmt/format.h>

#include <stdexcept>

namespace DB::Runtime::Wyrd
{

struct TraceRecord
{
    size_t step;
    UInt32 thread_id;
    std::string event;
    std::source_location location;
};


namespace
{

Simulation * current_simulation = nullptr;

std::string formatRecord(const TraceRecord & record)
{
    return fmt::format(
        "#{} thread {}: {} at {}:{}",
        record.step,
        record.thread_id,
        record.event,
        record.location.file_name(),
        record.location.line());
}

std::string formatTimeline(const std::vector<TraceRecord> & trace)
{
    std::string out;
    for (const auto & record : trace)
        out += formatRecord(record) + "\n";
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
        throw std::logic_error("Nested simulation");

    setRandomSeed(params.seed);
    createThread(std::move(main));
    SimulationResult::Status status = runLoop();

    return SimulationResult{
        .status = status,
        .exception_message = exception ? getExceptionMessage(exception, false) : std::string(),
        .total_steps = steps,
        .digest = digest.get64(),
        .timeline = formatTimeline(trace),
    };
}

SimulationResult::Status Simulation::runLoop()
{
    while (true)
    {
        SimulatedThread * next = scheduler.pickNext();
        if (!next)
            return alive == 0 ? SimulationResult::Status::Ok : SimulationResult::Status::Deadlock;

        if (steps >= params.step_limit)
            return SimulationResult::Status::StepLimit;

        ++steps;

        running_thread = next;
        current_simulation = this;
        next->resume();
        current_simulation = nullptr;
        running_thread = nullptr;

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
    trace.push_back(TraceRecord{.step = steps, .thread_id = running_thread->id, .event = std::move(event), .location = location});
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

    threads.push_back(std::make_unique<SimulatedThread>(next_thread_id++, params.stack_size, std::move(guarded_body)));
    ++alive;
    scheduler.onRunnable(threads.back().get());
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
    traceEvent("spawn", location);
    createThread(std::move(body));
}

bool Simulation::spuriousFailure(std::source_location location)
{
    traceEvent("spuriousFailure", location);
    return randomEventEvery(params.spurious_failure_every);
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

}
