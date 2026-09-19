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
        /// Fail closed at the cap: when set, reaching `read_no_more` is treated as a limit
        /// violation (an exception is thrown) if the underlying buffer still has data, instead of
        /// silently reporting EOF. Without this, content that ends exactly on the limit boundary
        /// followed by more data would be truncated silently rather than rejected. Default off,
        /// because some callers (e.g. a `Content-Length`-bounded HTTP body before a pipelined next
        /// request) intentionally rely on reporting EOF at the cap.
        bool throw_if_exceeded = false;
        std::string excetion_hint = {};
    };

    LimitReadBuffer(ReadBuffer & in_, Settings settings);
    LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, Settings settings);

    ~LimitReadBuffer() override;

    bool poll(size_t timeout_microseconds) override;

private:
    ReadBuffer * in;
    std::unique_ptr<ReadBuffer> holder;

    const Settings settings;

    LimitReadBuffer(ReadBuffer * in_, bool owns, size_t limit_, bool throw_exception_, std::optional<size_t> exact_limit_, std::string exception_message_);

    bool nextImpl() override;
    size_t getEffectiveBufferSize() const;
};

}
