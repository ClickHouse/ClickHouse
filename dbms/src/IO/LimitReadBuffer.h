#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    size_t limit;
    bool throw_exception;
    std::string exception_message;

    bool nextImpl() override;

public:
    LimitReadBuffer(ReadBuffer & in, size_t limit, bool throw_exception, std::string exception_message = {});
    ~LimitReadBuffer() override;
};

}
