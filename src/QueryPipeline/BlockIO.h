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

    BlockIO & operator= (BlockIO && rhs) noexcept;
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

    void onFinish()
    {
        if (finish_callback)
        {
            finish_callback(pipeline);
        }
        pipeline.reset();
    }

    void onException()
    {
        if (exception_callback)
            exception_callback();

        pipeline.reset();
    }

    /// Set is_all_data_sent in system.processes for this query.
    void setAllDataSent() const;

private:
    void reset();
};

}
