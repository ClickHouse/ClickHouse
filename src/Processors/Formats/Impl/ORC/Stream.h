#pragma once

#include <Processors/Formats/Impl/ORC/Compression.h>
#include <Common/PODArray.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace DB::ORC
{

/// A sequence of positions from a row index entry, consumed by the streams and the decoders of a
/// column in their order.
class PositionProvider
{
public:
    explicit PositionProvider(const std::vector<UInt64> & positions_) : positions(positions_) {}
    UInt64 next();

private:
    const std::vector<UInt64> & positions;
    size_t index = 0;
};

/// A piece of a stream loaded from the file. `data` is valid while `holder` is alive.
struct LoadedRange
{
    std::shared_ptr<const void> holder;
    std::string_view data;
};

/// Loads the range `(offset, length)` of a stream from the file.
using RangeLoader = std::function<LoadedRange(UInt64 offset, UInt64 length)>;

/// A stream of an ORC column, such as PRESENT or DATA, decompressed chunk by chunk. The decoders
/// read from the buffer `[pos, end)` of the current chunk directly.
class InputByteStream
{
public:
    /// The whole stream in memory. `data` must outlive the stream.
    InputByteStream(std::string_view data_, CompressionKind compression_, size_t block_size_, std::string name_);
    /// The stream of `length_` bytes is loaded from the file on demand, by `loader_`, a window of
    /// at least `LAZY_READ_SIZE` bytes at a time. Used when only some row groups are read.
    InputByteStream(RangeLoader loader_, UInt64 length_, CompressionKind compression_, size_t block_size_, std::string name_);

    static constexpr size_t LAZY_READ_SIZE = 1024 * 1024;

    /// Loads the next chunk. Returns false at the end of the stream.
    bool nextChunk();

    bool eof()
    {
        return pos == end && !nextChunk();
    }

    UInt8 readByte()
    {
        if (pos == end && !nextChunk())
            throwUnexpectedEnd();
        return static_cast<UInt8>(*pos++);
    }

    void readBytes(char * to, size_t n);
    void skipBytes(size_t n);

    /// Moves to the position from the row index: one value (the byte offset) for an uncompressed
    /// stream, two values (the offset of the compressed chunk and the offset in its decompressed
    /// data) for a compressed one.
    void seek(PositionProvider & positions);

    [[noreturn]] void throwUnexpectedEnd() const;

    const std::string & getName() const { return name; }

    const char * pos = nullptr;
    const char * end = nullptr;

private:
    /// Returns the range `[offset, offset + length)` of the stream. It is valid until the next call.
    std::string_view getRange(UInt64 offset, UInt64 length);

    /// The stream in memory, or the currently loaded window of a lazily loaded stream.
    std::string_view data;
    UInt64 length;
    RangeLoader loader;
    /// For a lazily loaded stream: the offset of `data` in the stream, and its owner.
    UInt64 window_offset = 0;
    std::shared_ptr<const void> window_holder;

    CompressionKind compression;
    size_t block_size;
    std::string name;

    /// The offset of the next chunk to read in `data`.
    size_t next_chunk_offset = 0;
    PODArray<char> decompressed;
};

/// Accumulates a stream of an ORC column, compressing it chunk by chunk.
class OutputByteStream
{
public:
    /// `compressor` can be nullptr, which means no compression.
    OutputByteStream(Compressor * compressor_, size_t block_size_);

    void writeByte(UInt8 byte)
    {
        if (buffer.size() == block_size)
            flushChunk();
        buffer.push_back(static_cast<char>(byte));
    }

    void write(const char * data, size_t size);

    /// Appends the current position to `positions`, in the format that `InputByteStream::seek` expects.
    void recordPosition(std::vector<UInt64> & positions) const;

    /// Finishes the stream and returns its size in bytes.
    size_t finish();

    const std::string & getOutput() const { return output; }

    /// Clears the stream to start a new stripe.
    void reset();

    size_t estimateSize() const { return output.size() + buffer.size(); }

private:
    void flushChunk();

    Compressor * compressor;
    size_t block_size;
    std::string buffer;
    std::string output;
};

}
