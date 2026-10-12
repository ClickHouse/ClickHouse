#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Common/VectorWithMemoryTracking.h>

#include <span>

namespace DB
{

namespace Runtime::V2
{

bool isLightWork(const ExecutingProcessor & record);
bool allLightWork(std::span<ExecutingProcessor * const> records);
void sortRunnable(VectorWithMemoryTracking<ExecutingProcessor *> & runnable, const ExecutingProcessor & picked);

}

}
