#pragma once

#include <Interpreters/Context.h>
#include <QueryCoordination/Optimizer/CBOSettings.h>

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

    ContextPtr getQueryContext() const;

    const CBOSettings & getCBOSettings() const;

    void pushTask(OptimizeTaskPtr task);

private:
    Memo & memo;
    Scheduler & scheduler;

    ContextPtr query_context;
    CBOSettings cbo_settings;
};

using OptimizeContextPtr = std::shared_ptr<OptimizeContext>;

}
