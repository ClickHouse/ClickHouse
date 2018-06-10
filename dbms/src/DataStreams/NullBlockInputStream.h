#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** Empty stream of blocks of specified structure.
  */
class NullBlockInputStream : public IBlockInputStream
{
public:
    NullBlockInputStream(const Block & header) : header(header) {}

    Block read() override { return {}; }
    Block getHeader() const override { return header; }
    String getName() const override { return "Null"; }

private:
    Block header;
};

}
