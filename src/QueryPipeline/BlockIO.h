#pragma once

#include <functional>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs);
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<ProcessListEntry> process_list_entry;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(QueryPipeline &)> finish_callback;
    std::function<void()> exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Call these functions if you want to log the request.
    void onFinish()
    {
        if (finish_callback)
        {
            finish_callback(pipeline);
        }
    }

    void onException() const
    {
        if (exception_callback)
            exception_callback();
    }

private:
    void reset();
};

}
