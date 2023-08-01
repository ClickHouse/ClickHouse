#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <IO/WriteBuffer.h>
#include <IO/PeekableWriteBuffer.h>

namespace DB
{

template <typename Base, typename... Args>
class RowOutputFormatWithExceptionHandlerAdaptorBase : public Base
{
public:
    RowOutputFormatWithExceptionHandlerAdaptorBase(bool handle_exceptions, const Block & header, WriteBuffer & out_, Args... args)
        : Base(header, out_, std::forward<Args>(args)...)
    {
        if (handle_exceptions)
            peekable_out = std::make_unique<PeekableWriteBuffer>(*Base::getWriteBufferPtr());
    }

    void write(const Columns & columns, size_t row_num)
    {
        if (!peekable_out)
            Base::write(columns, row_num);


        PeekableWriteBufferCheckpoint checkpoint(*peekable_out);
        try
        {
            Base::write(columns, row_num);
        }
        catch (...)
        {
            peekable_out->rollbackToCheckpoint();
            throw;
        }
    }

    void flush() override
    {
        getWriteBufferPtr()->next();

        if (peekable_out)
            Base::getWriteBufferPtr()->next();
    }

    void finalizeBuffers() override
    {
        if (peekable_out)
            peekable_out->finalize();
    }

    void resetFormatterImpl() override
    {
        peekable_out = std::make_unique<PeekableWriteBuffer>(*Base::getWriteBufferPtr());
    }

protected:
    /// Returns buffer that should be used in derived classes instead of out.
    WriteBuffer * getWriteBufferPtr() override
    {
        if (peekable_out)
            peekable_out.get();
        return Base::getWriteBufferPtr();
    }

private:

    std::unique_ptr<PeekableWriteBuffer> peekable_out;
};

}

