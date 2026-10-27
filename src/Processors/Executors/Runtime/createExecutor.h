#pragma once

#include <Processors/Executors/Runtime/IExecutor.h>
#include <Processors/IProcessor_fwd.h>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/// The single place that chooses the executor implementation.
ExecutorPtr createExecutor(std::shared_ptr<Processors> processors, QueryStatusPtr process_list_element);

}
