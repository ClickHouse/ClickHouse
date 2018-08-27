#pragma once

#include <math.h>

#include <vector>

#include <common/ThreadPool.h>
#include <IO/WriteBuffer.h>


namespace DB
{


/** Writes data asynchronously using double buffering.
  */
class AsynchronousWriteBuffer : public WriteBuffer
{
private:
    WriteBuffer & out;               /// The main buffer, responsible for writing data.
    std::vector <char> memory;       /// A piece of memory for duplicating the buffer.
    ThreadPool pool;                 /// For asynchronous data writing.
    bool started;                    /// Has an asynchronous data write started?

    /// Swap the main and duplicate buffers.
    void swapBuffers()
    {
        swap(out);
    }

    void nextImpl() override
    {
        if (!offset())
            return;

        if (started)
            pool.wait();
        else
            started = true;

        swapBuffers();

        /// The data will be written in separate stream.
        pool.schedule([this] { thread(); });
    }

public:
    AsynchronousWriteBuffer(WriteBuffer & out_) : WriteBuffer(nullptr, 0), out(out_), memory(out.buffer().size()), pool(1), started(false)
    {
        /// Data is written to the duplicate buffer.
        set(memory.data(), memory.size());
    }

    ~AsynchronousWriteBuffer() override
    {
        try
        {
            if (started)
                pool.wait();

            swapBuffers();
            out.next();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// That is executed in a separate thread
    void thread()
    {
        out.next();
    }
};

}
