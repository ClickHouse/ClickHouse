#include <Processors/QueryPlan/ReadFromQueryResultCacheStep.h>

namespace DB
{

ReadFromQueryResultCacheStep::ReadFromQueryResultCacheStep(Pipe pipe_)
    : ReadFromPreparedSource(std::move(pipe_))
{
}

}
