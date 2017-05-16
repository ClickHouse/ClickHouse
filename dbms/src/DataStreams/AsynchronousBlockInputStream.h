#pragma once

#include <Poco/Event.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/MemoryTracker.h>


namespace CurrentMetrics
{
    extern const Metric QueryThread;
}

namespace DB
{

/** Executes another BlockInputStream in a separate thread.
  * This serves two purposes:
  * 1. Allows you to make the different stages of the query execution pipeline work in parallel.
  * 2. Allows you not to wait until the data is ready, and periodically check their readiness without blocking.
  *    This is necessary, for example, so that during the waiting period you can check if a packet
  *     has come over the network with a request to interrupt the execution of the query.
  *    It also allows you to execute multiple queries at the same time.
  */
class AsynchronousBlockInputStream : public IProfilingBlockInputStream
{
public:
    AsynchronousBlockInputStream(BlockInputStreamPtr in_)
    {
        children.push_back(in_);
    }

    String getName() const override { return "Asynchronous"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Asynchronous(" << children.back()->getID() << ")";
        return res.str();
    }

    void readPrefix() override
    {
        /// Do not call `readPrefix` on the child, so that the corresponding actions are performed in a separate thread.
        if (!started)
        {
            next();
            started = true;
        }
    }

    void readSuffix() override
    {
        if (started)
        {
            pool.wait();
            if (exception)
                std::rethrow_exception(exception);
            children.back()->readSuffix();
            started = false;
        }
    }


    /** Wait for the data to be ready no more than the specified timeout. Start receiving data if necessary.
      * If the function returned true - the data is ready and you can do `read()`; You can not call the function just at the same moment again.
      */
    bool poll(UInt64 milliseconds)
    {
        if (!started)
        {
            next();
            started = true;
        }

        return ready.tryWait(milliseconds);
    }


    ~AsynchronousBlockInputStream() override
    {
        if (started)
            pool.wait();
    }

protected:
    ThreadPool pool{1};
    Poco::Event ready;
    bool started = false;
    bool first = true;

    Block block;
    std::exception_ptr exception;


    Block readImpl() override
    {
        /// If there were no calculations yet, calculate the first block synchronously
        if (!started)
        {
            calculate(current_memory_tracker);
            started = true;
        }
        else    /// If the calculations are already in progress - wait for the result
            pool.wait();

        if (exception)
            std::rethrow_exception(exception);

        Block res = block;
        if (!res)
            return res;

        /// Start the next block calculation
        block = Block();
        next();

        return res;
    }


    void next()
    {
        ready.reset();
        pool.schedule(std::bind(&AsynchronousBlockInputStream::calculate, this, current_memory_tracker));
    }


    /// Calculations that can be performed in a separate thread
    void calculate(MemoryTracker * memory_tracker)
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

        try
        {
            if (first)
            {
                first = false;
                setThreadName("AsyncBlockInput");
                current_memory_tracker = memory_tracker;
                children.back()->readPrefix();
            }

            block = children.back()->read();
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        ready.set();
    }
};

}

