#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    /** process_list_entry should be destroyed after in and after out,
      *  since in and out contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      */
    std::shared_ptr<ProcessListEntry> process_list_entry;

    BlockInputStreamPtr in;
    BlockOutputStreamPtr out;

    /// Callbacks for query logging could be set here.
    std::function<void(IBlockInputStream *, IBlockOutputStream *)>    finish_callback;
    std::function<void()>                                             exception_callback;

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

    /// We provide the correct order of destruction.
    void reset()
    {
        out.reset();
        in.reset();
        process_list_entry.reset();
    }

    BlockIO & operator= (const BlockIO & rhs)
    {
        reset();

        process_list_entry      = rhs.process_list_entry;
        in                      = rhs.in;
        out                     = rhs.out;

        finish_callback         = rhs.finish_callback;
        exception_callback      = rhs.exception_callback;

        return *this;
    }

    ~BlockIO();
};

}
