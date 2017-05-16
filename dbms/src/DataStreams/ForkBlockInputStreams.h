#pragma once

#include <DataStreams/QueueBlockIOStream.h>


namespace DB
{


/** Allows you to make several sources from one.
  * Used for single-pass execution of several queries at once.
  *
  * Multiple received sources should be read from different threads!
  * Uses O(1) RAM (does not buffer all data).
  * For this, readings from different sources are synchronized:
  *  reading of next block is blocked until all sources have read the current block.
  */
class ForkBlockInputStreams : private boost::noncopyable
{
public:
    ForkBlockInputStreams(BlockInputStreamPtr source_) : source(source_) {}

    /// Create a source. Call the function as many times as many forked sources you need.
    BlockInputStreamPtr createInput()
    {
        destinations.emplace_back(std::make_shared<QueueBlockIOStream>(1));
        return destinations.back();
    }

    /// Before you can read from the sources you have to "run" this construct.
    void run()
    {
        while (1)
        {
            if (destinations.empty())
                return;

            Block block = source->read();

            for (Destinations::iterator it = destinations.begin(); it != destinations.end();)
            {
                if ((*it)->isCancelled())
                {
                    destinations.erase(it++);
                }
                else
                {
                    (*it)->write(block);
                    ++it;
                }
            }

            if (!block)
                return;
        }
    }

private:
    /// From where to read.
    BlockInputStreamPtr source;

    /** Forked sources.
      * Made on the basis of a queue of small length.
      * A block from `source` is put in each queue.
      */
    using Destination = std::shared_ptr<QueueBlockIOStream>;
    using Destinations = std::list<Destination>;
    Destinations destinations;
};

using ForkPtr = std::shared_ptr<ForkBlockInputStreams>;
using Forks = std::vector<ForkPtr>;

}
