#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>
#include <Processors/IProcessor.h>

namespace DB::Runtime::V2
{

PoolMetrics runPipeline(Processors processors, size_t threads);

}
