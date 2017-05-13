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
        : generator(generator_) {}

    String getName() const override { return "Lazy"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Lazy(" << this << ")";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        if (!input)
        {
            input = generator();

            if (!input)
                return Block();

            children.push_back(input);

            if (IProfilingBlockInputStream * p_input = dynamic_cast<IProfilingBlockInputStream *>(input.get()))
            {
                /// They could have been set before, but were not passed into the `input`.
                if (progress_callback)
                    p_input->setProgressCallback(progress_callback);
                if (process_list_elem)
                    p_input->setProcessListElement(process_list_elem);
            }
        }

        return input->read();
    }

private:
    Generator generator;
    BlockInputStreamPtr input;
};

}
