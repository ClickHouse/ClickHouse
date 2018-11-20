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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wno-ignored-attributes"

    ReadBuffer & in [[maybe_unused]];

#pragma GCC diagnostic pop

public:
    IInputFormat(Block header, ReadBuffer & in)
        : ISource(std::move(header)), in(in)
    {
    }
};

}
