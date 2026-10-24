#pragma once

#include <Processors/Formats/Impl/ORC/Compression.h>
#include <Common/PODArray.h>

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

/// A stream of an ORC column, such as PRESENT or DATA, read from memory and decompressed chunk by
/// chunk. The decoders read from the buffer `[pos, end)` of the current chunk directly.
class InputByteStream
{
public:
    /// `data` must outlive the stream.
    InputByteStream(std::string_view data_, CompressionKind compression_, size_t block_size_, std::string name_);

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
    std::string_view data;
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
