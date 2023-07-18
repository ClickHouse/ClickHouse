#include "WaitForAsyncInsertSource.h"

#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISource.h>

#include <future>

namespace DB
{

Chunk WaitForAsyncInsertSource::generate()
{
    auto query_context = context.lock();
    auto * queue = query_context->getAsynchronousInsertQueue();
    auto insert_future = queue->pushNoCheck(query, query_context, std::move(bytes));
    if (wait)
    {
        auto status = insert_future.wait_for(std::chrono::milliseconds(timeout_ms));
        if (status == std::future_status::deferred)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: got future in deferred state");

        if (status == std::future_status::timeout)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded)", timeout_ms);
        insert_future.get();
    }
    return Chunk();
}
}
