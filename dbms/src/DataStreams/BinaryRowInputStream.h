#pragma once

#include <DataStreams/IRowInputStream.h>


namespace DB
{

class Block;
class ReadBuffer;


/** Поток для ввода данных в бинарном построчном формате.
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
