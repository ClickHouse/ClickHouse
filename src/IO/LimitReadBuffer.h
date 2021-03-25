#pragma once

#include <common/types.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  * Note that the nested ReadBuffer may read slightly more data internally to fill its buffer.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    UInt64 limit;
    bool throw_exception;
    std::string exception_message;

    bool nextImpl() override;

public:
    LimitReadBuffer(ReadBuffer & in_, UInt64 limit_, bool throw_exception_, std::string exception_message_ = {});
    ~LimitReadBuffer() override;
};

}
