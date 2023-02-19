#pragma once

#include <base/types.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  * Note that the nested ReadBuffer may read slightly more data internally to fill its buffer.
  */
class LimitReadBuffer : public ReadBuffer
{
public:
    LimitReadBuffer(ReadBuffer & in_, UInt64 limit_, bool throw_exception_, std::string exception_message_ = {});
    LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, UInt64 limit_, bool throw_exception_, std::string exception_message_ = {});
    ~LimitReadBuffer() override;

private:
    ReadBuffer * in;
    bool owns_in;

    UInt64 limit;
    bool throw_exception;
    std::string exception_message;

    LimitReadBuffer(ReadBuffer * in_, bool owns, UInt64 limit_, bool throw_exception_, std::string exception_message_);

    bool nextImpl() override;
};

}
