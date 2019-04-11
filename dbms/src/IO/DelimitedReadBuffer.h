#pragma once

#include <IO/ReadBuffer.h>
#include <Common/typeid_cast.h>

namespace DB
{
/// Consistently reads from one sub-buffer in a circle, and delimits its output with a character.
/// Owns sub-buffer.
class DelimitedReadBuffer : public ReadBuffer
{
public:
    DelimitedReadBuffer(ReadBuffer * buffer_, char delimiter_) : ReadBuffer(nullptr, 0), buffer(buffer_), delimiter(delimiter_)
    {
        // TODO: check that `buffer_` is not nullptr.
    }

    template <class BufferType>
    BufferType * subBufferAs()
    {
        return typeid_cast<BufferType *>(buffer.get());
    }

protected:
    // XXX: don't know how to guarantee that the next call to this method is done after we read all previous data.
    bool nextImpl() override
    {
        if (put_delimiter)
        {
            BufferBase::set(&delimiter, 1, 0);
            put_delimiter = false;
        }
        else
        {
            if (!buffer->next())
                return false;

            BufferBase::set(buffer->position(), buffer->available(), 0);
            put_delimiter = (delimiter != 0);
        }

        return true;
    }

private:
    std::unique_ptr<ReadBuffer> buffer; // FIXME: should be `const`, but `ReadBuffer` doesn't allow
    char delimiter; // FIXME: should be `const`, but `ReadBuffer` doesn't allow

    bool put_delimiter = false;
};

} // namespace DB
