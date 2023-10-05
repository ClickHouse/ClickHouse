#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>

#include <IO/WriteBuffer.h>
#include <IO/PeekableWriteBuffer.h>

namespace DB
{

template <typename Base, typename... Args>
class RowOutputFormatWithExceptionHandlerAdaptor : public Base
{
public:
    RowOutputFormatWithExceptionHandlerAdaptor(const Block & header, WriteBuffer & out_, bool handle_exceptions, Args... args)
        : Base(header, out_, std::forward<Args>(args)...)
    {
        if (handle_exceptions)
            peekable_out = std::make_unique<PeekableWriteBuffer>(*Base::getWriteBufferPtr());
    }

    void consume(DB::Chunk chunk) override
    {
        if (!peekable_out)
        {
            Base::consume(std::move(chunk));
            return;
        }

        auto num_rows = chunk.getNumRows();
        const auto & columns = chunk.getColumns();

        for (size_t row = 0; row < num_rows; ++row)
        {
            /// It's important to set a checkpoint before writing row-between delimiter
            peekable_out->setCheckpoint();

            if (Base::haveWrittenData())
                writeRowBetweenDelimiter();

            try
            {
                write(columns, row);
            }
            catch (...)
            {
                peekable_out->rollbackToCheckpoint(/*drop=*/true);
                throw;
            }
            peekable_out->dropCheckpoint();

            Base::first_row = false;
        }
    }

    void write(const Columns & columns, size_t row_num) override { Base::write(columns, row_num); }
    void writeRowBetweenDelimiter() override { Base::writeRowBetweenDelimiter(); }

    void flush() override
    {
        if (peekable_out)
            peekable_out->next();

        Base::flush();
    }

    void finalizeBuffers() override
    {
        if (peekable_out)
            peekable_out->finalize();
        Base::finalizeBuffers();
    }

    void resetFormatterImpl() override
    {
        Base::resetFormatterImpl();
        if (peekable_out)
            peekable_out = std::make_unique<PeekableWriteBuffer>(*Base::getWriteBufferPtr());
    }

    bool supportsWritingException() const override { return true; }

    void setException(const String & exception_message_) override { exception_message = exception_message_; }

protected:
    /// Returns buffer that should be used in derived classes instead of out.
    WriteBuffer * getWriteBufferPtr() override
    {
        if (peekable_out)
            return peekable_out.get();
        return Base::getWriteBufferPtr();
    }

    String exception_message;

private:

    std::unique_ptr<PeekableWriteBuffer> peekable_out;
};

}

