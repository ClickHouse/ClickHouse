#pragma once

#include <DataStreams/IBlockStream_fwd.h>

#include <functional>

#include <Processors/QueryPipeline.h>


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

    BlockOutputStreamPtr out;
    BlockInputStreamPtr in;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(IBlockInputStream *, IBlockOutputStream *)>    finish_callback;
    std::function<void()>                                             exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Call these functions if you want to log the request.
    void onFinish()
    {
        if (finish_callback)
            finish_callback(in.get(), out.get());
    }

    void onException()
    {
        if (exception_callback)
            exception_callback();
    }

    /// Returns in or converts pipeline to stream. Throws if out is not empty.
    BlockInputStreamPtr getInputStream();

private:
    void reset();
};

}
