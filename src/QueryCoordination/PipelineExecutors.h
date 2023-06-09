#pragma once

#include <Processors/Executors/PipelineExecutor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ThreadPool.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class PipelineExecutors
{
public:
    PipelineExecutors()
    {
        cleaner = std::make_unique<ThreadFromGlobalPool>(&PipelineExecutors::cleanerThread, this);
    }

    void execute(QueryPipeline & pipeline);
    void cleanerThread();

    struct Data;
    std::list<std::unique_ptr<Data>> executors;

    std::unique_ptr<ThreadFromGlobalPool> cleaner;

    bool shutdown = false;
};

}
