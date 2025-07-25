#pragma once
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
}

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
    ~ForkWriteBuffer() override;

protected:
    void nextImpl() override;
    void finalizeImpl() override;

private:
    WriteBufferPtrs sources;
};

}
