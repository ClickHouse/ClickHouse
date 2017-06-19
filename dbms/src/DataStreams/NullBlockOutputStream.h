#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Does nothing. Used for debugging and benchmarks.
  */
class NullBlockOutputStream : public IBlockOutputStream
{
public:
    void write(const Block & block) override {}
};

}
