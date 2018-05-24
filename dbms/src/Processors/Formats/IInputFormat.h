#pragma once

#include <Processors/ISource.h>


namespace DB
{

class ReadBuffer;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
private:
    ReadBuffer & in;

public:
    IInputFormat(Block header, ReadBuffer & in)
        : ISource(std::move(header)), in(in)
    {
    }
};

}
