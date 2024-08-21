#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <utility>
#include <memory>

namespace DB
{

class WriteBuffer;

/// WriteBuffer that decorates data and delegates it to underlying buffer.
/// It's used for writing compressed and encrypted data
template <class Base>
class WriteBufferDecorator : public Base
{
public:
    template <class ... BaseArgs>
    explicit WriteBufferDecorator(std::unique_ptr<WriteBuffer> out_, BaseArgs && ... args)
        : Base(std::forward<BaseArgs>(args)...), out(std::move(out_))
    {
    }

    void finalizeImpl() override
    {
        try
        {
            finalizeBefore();
            out->finalize();
            finalizeAfter();
        }
        catch (...)
        {
            /// Do not try to flush next time after exception.
            out->position() = out->buffer().begin();
            throw;
        }
    }

    WriteBuffer * getNestedBuffer() { return out.get(); }

protected:
    /// Do some finalization before finalization of underlying buffer.
    virtual void finalizeBefore() {}

    /// Do some finalization after finalization of underlying buffer.
    virtual void finalizeAfter() {}

    std::unique_ptr<WriteBuffer> out;
};

using WriteBufferWithOwnMemoryDecorator = WriteBufferDecorator<BufferWithOwnMemory<WriteBuffer>>;

}
