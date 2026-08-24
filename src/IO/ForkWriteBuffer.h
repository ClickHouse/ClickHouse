#pragma once

#include <IO/WriteBuffer.h>

#include <vector>

namespace DB
{

/** ForkWriteBuffer takes a vector of WriteBuffer and writes data to all of them
 * If the vector of WriteBufferPts is empty, then it throws an error
 * It uses the buffer of the first element as its buffer and copies data from
 * first buffer to all the other buffers
 **/
class ForkWriteBuffer : public WriteBuffer
{
public:
    using WriteBufferPtrs = std::vector<WriteBufferPtr>;
    explicit ForkWriteBuffer(WriteBufferPtrs && sources_);

protected:
    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

private:
    WriteBufferPtrs sources;
};

}
