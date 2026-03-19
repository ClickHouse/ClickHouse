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
template <typename Base>
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
            finalFlushBefore();
            out->finalize();
            finalFlushAfter();
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
        try
        {
            /// Try to flush compression buffers before cancelling.
            /// Such buffers don't guarantee that they are flushed on next(), although callers may expect so
            /// But if the out buffer is cancelled - it will get stuck
            bool out_buffer_still_valid = !out->isCanceled();
            if (out_buffer_still_valid)
                finalFlushBefore();
            this->next();
            Base::cancelImpl();
            out->next();
            out->cancel();
            if (out_buffer_still_valid)
                finalFlushAfter();
        }
        catch (...)
        {
            tryLogCurrentException("WriteBufferDecorator");
            out->cancel();
        }
    }

    WriteBuffer * getNestedBuffer() { return out; }

protected:
    /// Do some finalization before finalization/cancellation of underlying buffer.
    virtual void finalFlushBefore() {}

    /// Do some finalization after finalization/cancellation of underlying buffer.
    virtual void finalFlushAfter() {}

    std::unique_ptr<WriteBuffer> owning_holder;
    WriteBuffer * out;
};

using WriteBufferWithOwnMemoryDecorator = WriteBufferDecorator<BufferWithOwnMemory<WriteBuffer>>;

}
