#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeContext.h>
#include <QueryCoordination/Optimizer/Tasks/TaskContext.h>

namespace DB
{

class Memo;

class OptimizeTask
{
public:
    OptimizeTask(TaskContextPtr task_context);

    virtual ~OptimizeTask();

    virtual void execute() = 0;

    virtual String getDescription() = 0;

protected:
    void pushTask(std::unique_ptr<OptimizeTask> task);

    ContextPtr getQueryContext();

    TaskContextPtr task_context;
};

using OptimizeTaskPtr = std::unique_ptr<OptimizeTask>;

}
