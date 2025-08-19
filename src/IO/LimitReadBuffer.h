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
    LimitReadBuffer(ReadBuffer & in_, size_t limit_, bool throw_exception_,
                    std::optional<size_t> exact_limit_, std::string exception_message_ = {});
    LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t limit_, bool throw_exception_, std::optional<size_t> exact_limit_,
                    std::string exception_message_ = {});
    ~LimitReadBuffer() override;

private:
    ReadBuffer * in;
    const bool owns_in;

    const size_t limit;
    const bool throw_exception;
    const std::optional<size_t> exact_limit;
    const std::string exception_message;

    LoggerPtr log;

    LimitReadBuffer(ReadBuffer * in_, bool owns, size_t limit_, bool throw_exception_, std::optional<size_t> exact_limit_, std::string exception_message_);

    bool nextImpl() override;
};

}
