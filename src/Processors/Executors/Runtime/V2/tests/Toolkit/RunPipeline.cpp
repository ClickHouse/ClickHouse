#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/PipelineExecutor.h>

namespace DB::Runtime::V2
{

PoolMetrics runPipeline(Processors processors, size_t threads)
{
    PipelineExecutor executor(std::make_shared<Processors>(std::move(processors)), nullptr);
    executor.execute(threads, false);
    return executor.getMetrics();
}

}
