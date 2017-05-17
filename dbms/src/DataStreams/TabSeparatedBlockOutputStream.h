#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class Block;
class WriteBuffer;


/** Writes the data into a tab-separated file, but by columns, in blocks.
  * Blocks are separated by a double line feed.
  * On each row of the block - the data of one column.
  */
class TabSeparatedBlockOutputStream : public IBlockOutputStream
{
public:
    TabSeparatedBlockOutputStream(WriteBuffer & ostr_) : ostr(ostr_) {}

    void write(const Block & block) override;
    void flush() override;

private:
    WriteBuffer & ostr;
};

}
