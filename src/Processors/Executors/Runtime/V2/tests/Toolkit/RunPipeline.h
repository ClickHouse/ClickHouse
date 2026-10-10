#pragma once

#include <Processors/IProcessor.h>

namespace DB::Runtime::V2
{

void runPipeline(Processors processors, size_t threads);

}
