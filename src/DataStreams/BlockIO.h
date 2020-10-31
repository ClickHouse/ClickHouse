#pragma once

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <functional>
#include <memory>


namespace DB
{

class QueryProcess;

struct BlockIO
{
    BlockIO();
    BlockIO(BlockIO &&);

    BlockIO & operator= (BlockIO && rhs);
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<QueryProcess> query_process;

    BlockOutputStreamPtr out;
    BlockInputStreamPtr in;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(IBlockInputStream *, IBlockOutputStream *, QueryPipeline *)>    finish_callback;
    std::function<void()>                                                              exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Call these functions if you want to log the request.
    void onFinish()
    {
        if (finish_callback)
        {
            QueryPipeline * pipeline_ptr = nullptr;
            if (pipeline.initialized())
                pipeline_ptr = &pipeline;

            finish_callback(in.get(), out.get(), pipeline_ptr);
        }
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
