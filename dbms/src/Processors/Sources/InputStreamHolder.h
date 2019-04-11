#pragma once

#include <Core/Block.h>
#include <mutex>

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class InputStreamHolder
{
public:
    explicit InputStreamHolder(BlockInputStreamPtr stream_) : stream(std::move(stream_)) {}

    /// Unsafe. Only for single thread.
    Block read();

    /// Safe for multiple threads.
    void readSuffix();

    bool isFinished() const { return stream_finished; }

    IBlockInputStream & getStream() { return *stream; }
    const IBlockInputStream & getStream() const { return *stream; }

private:

    bool initialized = false;
    bool stream_finished = false;

    BlockInputStreamPtr stream;
    std::mutex lock;
};

using InputStreamHolderPtr = std::shared_ptr<InputStreamHolder>;
using InputStreamHolders = std::vector<InputStreamHolderPtr>;

}
