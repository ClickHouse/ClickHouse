#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Initialize another source on the first `read` call, and then use it.
  * This is needed, for example, to read from a table that will be populated
  *  after creation of LazyBlockInputStream object, but before the first `read` call.
  */
class LazyBlockInputStream : public IProfilingBlockInputStream
{
public:
    using Generator = std::function<BlockInputStreamPtr()>;

    LazyBlockInputStream(Generator generator_)
        : generator(std::move(generator_))
    {
    }

    LazyBlockInputStream(const char * name_, Generator generator_)
        : name(name_)
        , generator(std::move(generator_))
    {
    }

    String getName() const override { return name; }

    void cancel() override
    {
        std::lock_guard<std::mutex> lock(cancel_mutex);
        IProfilingBlockInputStream::cancel();
    }

protected:
    Block readImpl() override
    {
        if (!input)
        {
            input = generator();

            if (!input)
                return Block();

            auto * p_input = dynamic_cast<IProfilingBlockInputStream *>(input.get());

            if (p_input)
            {
                /// They could have been set before, but were not passed into the `input`.
                if (progress_callback)
                    p_input->setProgressCallback(progress_callback);
                if (process_list_elem)
                    p_input->setProcessListElement(process_list_elem);
            }

            input->readPrefix();

            {
                std::lock_guard<std::mutex> lock(cancel_mutex);

                /** TODO Data race here. See IProfilingBlockInputStream::collectAndSendTotalRowsApprox.
                    Assume following pipeline:

                    RemoteBlockInputStream
                     AsynchronousBlockInputStream
                      LazyBlockInputStream

                    RemoteBlockInputStream calls AsynchronousBlockInputStream::readPrefix
                     and AsynchronousBlockInputStream spawns a thread and returns.

                    The separate thread will call LazyBlockInputStream::read
                     LazyBlockInputStream::read will add more children to itself

                    In the same moment, in main thread, RemoteBlockInputStream::read is called,
                     then IProfilingBlockInputStream::collectAndSendTotalRowsApprox is called
                     and iterates over set of children.
                  */
                children.push_back(input);

                if (isCancelled() && p_input)
                    p_input->cancel();
            }
        }

        return input->read();
    }

private:
    const char * name = "Lazy";
    Generator generator;

    BlockInputStreamPtr input;

    std::mutex cancel_mutex;
};

}
