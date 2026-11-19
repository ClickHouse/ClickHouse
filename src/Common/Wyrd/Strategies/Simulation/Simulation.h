#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/FutexTable.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Scheduler.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulationParams.h>
#include <Common/SipHash.h>
#include <Common/VectorWithMemoryTracking.h>

#include <exception>
#include <functional>
#include <memory>
#include <source_location>
#include <string>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;
struct TraceRecord;

struct SimulationResult
{
    enum class Status
    {
        Ok,
        Deadlock,
        Exception,
        StepLimit,
        BrokenInvariant,
    };

    Status status;
    std::string exception_message;
    size_t total_steps;
    UInt64 digest;
    std::string timeline;
};

class Simulation
{
    SimulationResult::Status runLoop();
    void traceEvent(std::string event, std::source_location location);
    void createThread(std::function<void()> user_body);
    void leakUnfinishedThreads();

public:
    explicit Simulation(SimulationParams params_);
    ~Simulation();

    /// Test entry point
    SimulationResult run(std::function<void()> main);
    static Simulation * current();

    /// Syscalls
    void sync(const char * operation, std::source_location location);
    void yield(std::source_location location);
    void futexWait(void * address, std::source_location location);
    void futexWake(void * address, size_t count, std::source_location location);
    void spawn(std::function<void()> body, std::source_location location);
    bool spuriousFailure(std::source_location location);
    void enterLockFreeScope(std::source_location location);
    void leaveLockFreeScope(std::source_location location);
    void progressHint(std::source_location location);
    void trace(std::string message);
    void brokenInvariant(std::string message);

private:
    const SimulationParams params;
    Scheduler scheduler;

    VectorWithMemoryTracking<TraceRecord> timeline;

    FutexTable futexes;
    VectorWithMemoryTracking<std::unique_ptr<SimulatedThread>> threads;
    SimulatedThread * running_thread = nullptr;
    size_t alive = 0;
    UInt32 next_thread_id = 1;

    size_t steps = 0;
    SipHash digest;
    std::exception_ptr exception;
    std::string broken_invariant;
};

}
