#include <Processors/QueryPlan/ReadFromQueryCacheStep.h>

namespace DB
{

ReadFromQueryCacheStep::ReadFromQueryCacheStep(Pipe pipe_)
    : ReadFromPreparedSource(std::move(pipe_))
{
}

}
