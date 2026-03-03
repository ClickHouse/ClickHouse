#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

}

void CommonSubplanReferenceStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CommonSubplanReference cannot be used to build pipeline");
}

}
