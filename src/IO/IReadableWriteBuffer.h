#pragma once
#include <memory>
#include <IO/ReadBuffer.h>

namespace DB
{

struct IReadableWriteBuffer
{
    /// At the first time returns getReadBufferImpl(). Next calls return nullptr.
    std::unique_ptr<ReadBuffer> tryGetReadBuffer()
    {
        if (!can_reread)
            return nullptr;

        can_reread = false;
        return getReadBufferImpl();
    }

    virtual ~IReadableWriteBuffer() = default;

protected:

    /// Creates read buffer from current write buffer.
    /// Returned buffer points to the first byte of original buffer.
    /// Original stream becomes invalid.
    virtual std::unique_ptr<ReadBuffer> getReadBufferImpl() = 0;

    bool can_reread = true;
};

}
