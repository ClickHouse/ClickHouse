#pragma once

#include <Core/Block.h>
#include <Common/Exception.h>

namespace DB
{

using BlockPtr = std::shared_ptr<Block>;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class ReadBuffer;

/** Input format header reads data prefix from ReadBuffer and creates block header from it.
  */
class IInputFormatHeader
{
protected:

    /// Skip GCC warning: ‘maybe_unused’ attribute ignored
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"

    ReadBuffer & in [[maybe_unused]];
    BlockPtr header;

#pragma GCC diagnostic pop

public:
    IInputFormatHeader(ReadBuffer & in_);

    IInputFormatHeader(const IInputFormatHeader & other);

    virtual void readPrefix()
    {
        throw Exception("readPrefix is not implemented for IInputFormatHeader", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual const Block & getHeader() const
    {
        if (header)
            return *header;

        throw Exception("The header block is empty. Call readPrefix first", ErrorCodes::LOGICAL_ERROR);
    }

    virtual ~IInputFormatHeader() = default;
};

}
