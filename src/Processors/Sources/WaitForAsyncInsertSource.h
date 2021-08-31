#pragma once

#include <Processors/ISource.h>
#include <Interpreters/Context.h>

namespace DB
{

class WaitForAsyncInsertSource : public ISource, WithContext
{
public:
    WaitForAsyncInsertSource(
        const Block & header, const String & query_id_,
        size_t timeout_ms_, ContextPtr context_)
        : ISource(std::move(header))
        , WithContext(context_)
        , query_id(query_id_)
        , timeout_ms(timeout_ms_)
    {
    }

    String getName() const override { return "WaitForAsyncInsert"; }

protected:
    Chunk generate() override
    {
        auto context = getContext();
        auto * queue = context->getAsynchronousInsertQueue();
        assert(queue);
        queue->waitForProcessingQuery(query_id, std::chrono::milliseconds(timeout_ms));
        return Chunk();
    }

private:
    String query_id;
    size_t timeout_ms;
};

}
