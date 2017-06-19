#pragma once

#include <DataStreams/IRowInputStream.h>


namespace DB
{

class Block;
class ReadBuffer;


/** A stream for inputting data in a binary line-by-line format.
  */
class BinaryRowInputStream : public IRowInputStream
{
public:
    BinaryRowInputStream(ReadBuffer & istr_);

    bool read(Block & block) override;

private:
    ReadBuffer & istr;
};

}
