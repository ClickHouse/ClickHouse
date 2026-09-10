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

    bool poll(size_t timeout_microseconds) override;

    /// The bytes that may still be read before the limit, whatever the underlying buffer has cached so
    /// far. A reader uses this to reject a count that cannot fit the frame before it drives an allocation;
    /// unlike `available()`, it does not shrink to the current chunk.
    size_t bytesUntilLimit() const
    {
        return settings.read_no_more > count() ? settings.read_no_more - count() : 0;
    }

private:
    ReadBuffer * in;
    std::unique_ptr<ReadBuffer> holder;

    const Settings settings;

    LimitReadBuffer(ReadBuffer * in_, bool owns, size_t limit_, bool throw_exception_, std::optional<size_t> exact_limit_, std::string exception_message_);

    bool nextImpl() override;
    size_t getEffectiveBufferSize() const;
};

/// The bytes a reader can still take from the current frame, used to refuse a count of fixed-size
/// elements before it drives an allocation. A frame is a `LimitReadBuffer`, and its remaining bytes
/// are the bytes before its limit. The framed reader wraps every payload, outline and set in one, so
/// the bound applies wherever the input is untrusted. Without such a frame the remaining length is
/// unknown, so there is no bound to enforce here: `available` would be only the bytes buffered so far
/// and would wrongly refuse a valid count that crosses a buffer boundary on a streamed read.
inline size_t bytesRemainingInFrame(const ReadBuffer & in)
{
    if (const auto * limited = dynamic_cast<const LimitReadBuffer *>(&in))
        return limited->bytesUntilLimit();
    return std::numeric_limits<size_t>::max();
}

}
