#pragma once

#include <limits>
#include <memory>
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
    struct Settings
    {
        size_t read_no_less = 0;
        size_t read_no_more = std::numeric_limits<size_t>::max();
        bool expect_eof = false;
        std::string excetion_hint = {};
    };

    LimitReadBuffer(ReadBuffer & in_, Settings settings);
    LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, Settings settings);

    ~LimitReadBuffer() override;

private:
    ReadBuffer * in;
    std::unique_ptr<ReadBuffer> holder;

    const Settings settings;

    LimitReadBuffer(ReadBuffer * in_, bool owns, size_t limit_, bool throw_exception_, std::optional<size_t> exact_limit_, std::string exception_message_);

    bool nextImpl() override;
    size_t getEffectiveBufferSize() const;
};

}
