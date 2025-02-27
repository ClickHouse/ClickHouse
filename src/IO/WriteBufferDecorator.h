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
/// This class can own or not own underlying buffer - constructor will differentiate
/// std::unique_ptr<WriteBuffer> for owning and WriteBuffer* for not owning.
template <class Base>
class WriteBufferDecorator : public Base
{
public:
    template <class ... BaseArgs>
    explicit WriteBufferDecorator(std::unique_ptr<WriteBuffer> out_, BaseArgs && ... args)
        : Base(std::forward<BaseArgs>(args)...), owning_holder(std::move(out_)), out(owning_holder.get())
    {
    }

    template <class ... BaseArgs>
    explicit WriteBufferDecorator(WriteBuffer * out_, BaseArgs && ... args)
        : Base(std::forward<BaseArgs>(args)...), out(out_)
    {
    }

    void finalizeImpl() override
    {
        Base::finalizeImpl();
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

    void cancelImpl() noexcept override
    {
        out->cancel();
    }

    WriteBuffer * getNestedBuffer() { return out; }

protected:
    /// Do some finalization before finalization of underlying buffer.
    virtual void finalizeBefore() {}

    /// Do some finalization after finalization of underlying buffer.
    virtual void finalizeAfter() {}

    std::unique_ptr<WriteBuffer> owning_holder;
    WriteBuffer * out;
};

using WriteBufferWithOwnMemoryDecorator = WriteBufferDecorator<BufferWithOwnMemory<WriteBuffer>>;

}
