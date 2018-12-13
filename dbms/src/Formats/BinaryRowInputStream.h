#pragma once

#include <Formats/IRowInputStream.h>
#include <Core/Block.h>


namespace DB
{

class ReadBuffer;


/** A stream for inputting data in a binary line-by-line format.
  */
class BinaryRowInputStream : public IRowInputStream
{
public:
    BinaryRowInputStream(ReadBuffer & istr_, const Block & header_);

    bool read(MutableColumns & columns, RowReadExtension &) override;

private:
    ReadBuffer & istr;
    Block header;
};

}
