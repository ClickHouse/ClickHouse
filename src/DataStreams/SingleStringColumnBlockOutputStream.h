#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SingleStringColumnBlockOutputStream : public IBlockOutputStream
{
public:
    SingleStringColumnBlockOutputStream(WriteBuffer & buf_, Block header_) : buf(buf_), header(std::move(header_))
    {
        checkBlock(header);
    }

    Block getHeader() const override { return header; }
    void write(const Block & block) override
    {
        checkBlock(block);
        const auto * column = typeid_cast<const ColumnString *>(block.getByPosition(0).column.get());
        size_t size = column->size();
        for (size_t row = 0; row < size; ++row)
        {
            writeString(column->getDataAt(row), buf);
            writeChar('\n', buf);
        }

        buf.next();
    }

    void flush() override { buf.next(); }
    void writePrefix() override {}
    void writeSuffix() override {}

private:
    WriteBuffer & buf;
    Block header;

    static void checkBlock(const Block & block)
    {
        if (block.columns() != 1 || typeid_cast<const ColumnString *>(block.getByPosition(0).column.get()) == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "SingleStringColumnBlockOutputStream expect single string column in block. Got {}",
                            block.dumpStructure());
    }
};

}
