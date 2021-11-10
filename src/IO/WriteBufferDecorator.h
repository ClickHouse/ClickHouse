#pragma once

#include <IO/WriteBuffer.h>
#include <utility>
#include <memory>

namespace DB
{

class WriteBuffer;

/// WriteBuffer that works with another WriteBuffer.
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
            finalizeBeforeNestedFinalize();
            out->finalize();
            finalizeAfterNestedFinalize();
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
    virtual void finalizeBeforeNestedFinalize() {}
    virtual void finalizeAfterNestedFinalize() {}

    std::unique_ptr<WriteBuffer> out;
};

using WriteBufferWithOwnMemoryDecorator = WriteBufferDecorator<BufferWithOwnMemory<WriteBuffer>>;

}
