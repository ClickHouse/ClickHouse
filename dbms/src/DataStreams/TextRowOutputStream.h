#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <IO/QueueReadWriteBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{
/// Only used for INSERT INTO SELECT FROM FORMAT statements
class TextRowOutputStream : public IBlockOutputStream
{
public:
    TextRowOutputStream(const Block & sample_) : sample(sample_) {}

    Block getHeader() const override { return sample; }

    void write(const Block & block) override;

    void flush() override { wb.next(); }

    void writeSuffix() override { wb.flush(); }

    ReadBuffer & getReadBuffer()
    {
        rb = wb.tryGetReadBuffer();
        return *rb;
    }

private:
    Block sample;
    QueueWriteBuffer wb;
    std::shared_ptr<ReadBuffer> rb;
};

class TextDoCopyBlockInputStream : public IProfilingBlockInputStream
{
public:
    TextDoCopyBlockInputStream(const BlockInputStreamPtr & input_,  BlockOutputStreamPtr output_)
        : input(input_), output(output_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "TextDoCopy"; }

    Block getHeader() const override { return output->getHeader(); }

protected:
    Block readImpl() override
    {
        copyData(*input, *output);
        return Block();
    }

private:
    BlockInputStreamPtr input;
    BlockOutputStreamPtr output;
};


}
