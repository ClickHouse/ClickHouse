#include <Server/ArrowFlight/PollSession.h>

#if USE_ARROWFLIGHT

#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace ArrowFlight
{

PollSession::PollSession(
    ContextPtr query_context_,
    ThreadGroupPtr thread_group_,
    BlockIO && block_io_,
    SchemaModifier schema_modifier,
    BlockModifier block_modifier_)
    : query_context(query_context_)
    , thread_group(thread_group_)
    , block_io(std::move(block_io_))
    , block_modifier(block_modifier_)
{
    try
    {
        executor.emplace(block_io.pipeline);
        auto schema_res = flightSchema(executor->getHeader(), query_context, schema_modifier);
        if (!schema_res.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to convert Arrow schema: {}", schema_res.status().ToString());
        schema = std::move(schema_res).ValueUnsafe();
    }
    catch (...)
    {
        try { block_io.onException(); }
        catch (...) { tryLogCurrentException("PollSession: block_io.onException() failed during constructor rollback"); }
        throw;
    }
}

PollSession::~PollSession() = default;

ContextPtr PollSession::queryContext() { return query_context; }

ThreadGroupPtr PollSession::getThreadGroup() const { return thread_group; }

const String & PollSession::getSchema() const { return schema; }

bool PollSession::getNextBlock(Block & block)
{
    if (!executor->pull(block))
        return false;
    removeSpecialColumnRepresentations(block);
    if (block_modifier)
        block_modifier(query_context, block);
    return true;
}

void PollSession::onFinish() { block_io.onFinish(); }

void PollSession::onException() { block_io.onException(); }

void PollSession::onCancelOrConnectionLoss() { block_io.onCancelOrConnectionLoss(); }

}
}

#endif
