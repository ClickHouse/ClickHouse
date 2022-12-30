#pragma once

#include <Processors/ISource.h>
#include <Interpreters/AsynchronousInsertQueue.h>

namespace DB
{

/// Source, that allow to wait until processing of
/// asynchronous insert for specified query_id will be finished.
class WaitForAsyncInsertSource : public ISource, WithContext
{
public:
    WaitForAsyncInsertSource(
        const String & query_id_, size_t timeout_ms_, AsynchronousInsertQueue & queue_)
        : ISource(Block())
        , query_id(query_id_)
        , timeout_ms(timeout_ms_)
        , queue(queue_)
    {
    }

    String getName() const override { return "WaitForAsyncInsert"; }

protected:
    Chunk generate() override
    {
        queue.waitForProcessingQuery(query_id, std::chrono::milliseconds(timeout_ms));
        return Chunk();
    }

private:
    String query_id;
    size_t timeout_ms;
    AsynchronousInsertQueue & queue;
};

}
