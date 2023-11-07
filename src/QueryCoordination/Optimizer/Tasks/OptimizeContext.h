#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Memo;
class Scheduler;
class OptimizeTask;
using OptimizeTaskPtr = std::unique_ptr<OptimizeTask>;


class OptimizeContext
{
public:
    OptimizeContext(Memo & memo_, Scheduler & scheduler_, ContextPtr query_context_);

    Memo & getMemo();

    ContextPtr getQueryContext();

    void pushTask(OptimizeTaskPtr task);

private:
    Memo & memo;
    Scheduler & scheduler;

    ContextPtr query_context;
};

using OptimizeContextPtr = std::shared_ptr<OptimizeContext>;

}
