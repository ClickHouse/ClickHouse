#pragma once

#include <memory>

class MemoryTracker;

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ReadProgressCallback;
class StepProfiler;
struct MemoryReservation;

namespace Runtime::V2
{

struct ExecutionOptions
{
    QueryStatusPtr process_list_element;
    MemoryReservation * reservation = nullptr;
    MemoryTracker * tracker = nullptr;
    ReadProgressCallback * read_progress_callback = nullptr;
    StepProfiler * step_profiler = nullptr;
    bool profile_processors = false;
    bool trace_processors = false;
};

ExecutionOptions makeExecutionOptions(QueryStatusPtr process_list_element);

}

}
