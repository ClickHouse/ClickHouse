#pragma once

#include <Optimizer/Tasks/OptimizeTask.h>
#include <Poco/Timestamp.h>
#include <stack>

namespace DB
{

class Scheduler
{
public:
    explicit Scheduler(UInt64 max_run_time_ms_) : start_time_ms(Poco::Timestamp()), max_run_time_ms(max_run_time_ms_) { }

    /// Execute one task in stack
    void run();
    void pushTask(OptimizeTaskPtr task);

    UInt64 getRunCount() { return run_count; }

private:
    Poco::Timestamp start_time_ms;
    UInt64 max_run_time_ms;

    /// How many times method 'run' executes.
    UInt64 run_count;
    std::stack<OptimizeTaskPtr> stack;
};

}
