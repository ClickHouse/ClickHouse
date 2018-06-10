#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Does nothing. Used for debugging and benchmarks.
  */
class NullBlockOutputStream : public IBlockOutputStream
{
public:
    NullBlockOutputStream(const Block & header) : header(header) {}
    Block getHeader() const override { return header; }
    void write(const Block &) override {}

private:
    Block header;
};

}
