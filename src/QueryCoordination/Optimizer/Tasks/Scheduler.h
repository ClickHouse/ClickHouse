#pragma once

#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class Scheduler
{
public:
    void run();

    void pushTask(OptimizeTaskPtr task);

private:
    std::stack<OptimizeTaskPtr> stack;
};

}
