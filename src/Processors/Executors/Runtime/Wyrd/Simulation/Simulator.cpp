#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulator.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/SimulatedThread.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Common/Exception.h>

#include <fmt/format.h>

#include <stdexcept>

namespace DB::Runtime::Wyrd
{

struct SyscallRecord
{
    size_t step;
    UInt32 thread_id;
    const char * operation;
    std::source_location location;
};


namespace
{

Simulator * current_simulator = nullptr;

std::string formatRecord(const SyscallRecord & record)
{
    return fmt::format(
        "#{} thread {}: {} at {}:{}\n",
        record.step,
        record.thread_id,
        record.operation,
        record.location.file_name(),
        record.location.line());
}

std::string formatTail(const std::vector<SyscallRecord> & trace, size_t count)
{
    std::string out;
    for (size_t i = trace.size() > count ? trace.size() - count : 0; i < trace.size(); ++i)
        out += formatRecord(trace[i]);
    return out;
}

}

Simulator::Simulator(SimulatorParams params_, SchedulerParams scheduler_params)
    : params(params_)
    , scheduler(scheduler_params)
{
}

Simulator::~Simulator() = default;

SimulatorResult Simulator::run(std::function<void()> main)
{
    if (current_simulator)
        throw std::logic_error("Nested simulation");

    setRandomSeed(params.seed);
    createThread(std::move(main));
    SimulatorResult::Status status = runLoop();

    return SimulatorResult{
        .status = status,
        .exception_message = exception ? getExceptionMessage(exception, false) : std::string(),
        .total_steps = steps,
        .digest = digest.get64(),
        .last_syscalls = formatTail(trace, params.printed_syscalls),
    };
}

SimulatorResult::Status Simulator::runLoop()
{
    while (true)
    {
        SimulatedThread * next = scheduler.pickNext();
        if (!next)
            return alive == 0 ? SimulatorResult::Status::Ok : SimulatorResult::Status::Deadlock;

        if (steps >= params.step_limit)
            return SimulatorResult::Status::StepLimit;

        ++steps;

        running_thread = next;
        current_simulator = this;
        next->resume();
        current_simulator = nullptr;
        running_thread = nullptr;

        if (next->finished())
        {
            if (exception)
                return SimulatorResult::Status::Exception;
            --alive;
            scheduler.onExit();
        }
    }
}

void Simulator::recordSyscall(const char * operation, std::source_location location)
{
    digest.update(running_thread->id);
    digest.update(location.line());
    trace.push_back(SyscallRecord{.step = steps, .thread_id = running_thread->id, .operation = operation, .location = location});
}

void Simulator::createThread(std::function<void()> user_body)
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

Simulator * Simulator::current()
{
    return current_simulator;
}

void Simulator::sync(const char * operation, std::source_location location)
{
    recordSyscall(operation, location);
    scheduler.onSync();
    running_thread->suspend();
}

void Simulator::yield(std::source_location location)
{
    recordSyscall("yield", location);
    scheduler.onYield();
    running_thread->suspend();
}

void Simulator::futexWait(void * address, std::source_location location)
{
    recordSyscall("futexWait", location);
    if (randomEventEvery(params.spurious_wakeup_every))
    {
        scheduler.onYield();
    }
    else
    {
        futexes.park(address, running_thread);
        scheduler.onSuspend();
    }
    running_thread->suspend();
}

void Simulator::futexWake(void * address, size_t count, std::source_location location)
{
    recordSyscall("futexWake", location);
    for (SimulatedThread * waiter : futexes.wake(address, count))
        scheduler.onRunnable(waiter);
}

void Simulator::spawn(std::function<void()> body, std::source_location location)
{
    recordSyscall("spawn", location);
    createThread(std::move(body));
}

bool Simulator::spuriousFailure(std::source_location location)
{
    recordSyscall("spuriousFailure", location);
    return randomEventEvery(params.spurious_failure_every);
}

}
