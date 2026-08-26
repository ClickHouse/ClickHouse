#pragma once

#include "config.h"

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/commandSelector.h>

#include <Common/ThreadStatus.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>

#include <arrow/type.h>

#include <optional>


namespace DB
{

namespace ArrowFlight
{

/// Keeps a query context and a pipeline executor for PollFlightInfo.
class PollSession
{
public:
    PollSession(
        ContextPtr query_context_,
        ThreadGroupPtr thread_group_,
        BlockIO && block_io_,
        SchemaModifier schema_modifier = nullptr,
        BlockModifier block_modifier_ = nullptr);

    ~PollSession();

    ContextPtr queryContext();

    ThreadGroupPtr getThreadGroup() const;
    std::shared_ptr<arrow::Schema> getSchema() const;
    bool getNextBlock(Block & block);
    void onFinish();
    void onException();
    void onCancelOrConnectionLoss();

private:
    ContextPtr query_context;
    ThreadGroupPtr thread_group;
    BlockIO block_io;
    std::optional<PullingPipelineExecutor> executor;
    std::shared_ptr<arrow::Schema> schema;
    BlockModifier block_modifier;
};

}
}

#endif
