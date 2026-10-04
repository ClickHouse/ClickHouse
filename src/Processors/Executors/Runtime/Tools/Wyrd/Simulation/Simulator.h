#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/FutexTable.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Scheduler.h>
#include <Common/CoroutineStack.h>

#include <exception>
#include <functional>
#include <memory>
#include <source_location>
#include <string>
#include <vector>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;
struct SyscallRecord;

struct SimulatorResult
{
    enum class Status
    {
        Ok,
        Deadlock,
        Exception,
        StepLimit,
    };

    Status status;
    std::string exception_message;
    size_t total_steps;
    UInt64 digest;
    std::string last_syscalls;
};

struct SimulatorParams
{
    UInt64 seed = 42;
    size_t spurious_wakeup_every = 7;
    size_t spurious_failure_every = 7;
    size_t step_limit = 100'000;
    size_t printed_syscalls = 100;
    size_t stack_size = CoroutineStack::default_stack_size;
};

class Simulator
{
    SimulatorResult::Status runLoop();
    void recordSyscall(const char * operation, std::source_location location);
    void createThread(std::function<void()> user_body);

public:
    Simulator(SimulatorParams params_, SchedulerParams scheduler_params);
    ~Simulator();

    /// Test entry point
    SimulatorResult run(std::function<void()> main);
    static Simulator * current();

    /// Syscalls
    void sync(const char * operation, std::source_location location);
    void yield(std::source_location location);
    void futexWait(void * address, std::source_location location);
    void futexWake(void * address, size_t count, std::source_location location);
    void spawn(std::function<void()> body, std::source_location location);
    bool spuriousFailure(std::source_location location);

private:
    const SimulatorParams params;
    Scheduler scheduler;

    std::vector<SyscallRecord> trace;

    FutexTable futexes;
    std::vector<std::unique_ptr<SimulatedThread>> threads;
    SimulatedThread * running_thread = nullptr;
    size_t alive = 0;
    UInt32 next_thread_id = 1;

    size_t steps = 0;
    UInt64 digest_state = 0;
    std::exception_ptr exception;
};

}
