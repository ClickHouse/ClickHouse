#pragma once

#include <IO/IReadableWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <string_view>

namespace DB
{

/// A write buffer keeping its bytes in memory, in pieces that grow with the data: the first piece doubles in place up to
/// MIN_PIECE_SIZE, and each later piece is the bytes written so far divided by PIECE_SIZE_DIVISOR, within
/// [MIN_PIECE_SIZE, MAX_PIECE_SIZE]. The memory allocated ahead of the data is at most one piece, and growth copies at most
/// MIN_PIECE_SIZE bytes. The read buffer of IReadableWriteBuffer takes the memory over and frees it as it reads.
class PiecewiseWriteBuffer : public WriteBuffer, public IReadableWriteBuffer
{
public:
    PiecewiseWriteBuffer();
    ~PiecewiseWriteBuffer() override;

    /// A read buffer over the bytes written so far, which stay in this buffer; valid until the next write.
    std::unique_ptr<ReadBuffer> readWritten() const;

    /// Frees the memory and starts again from no bytes; the buffer stays the same object.
    void clear();

    size_t allocatedBytes() const;

private:
    static constexpr size_t INITIAL_PIECE_SIZE = 64;
    static constexpr size_t MIN_PIECE_SIZE = 1024;
    static constexpr size_t MAX_PIECE_SIZE = 1024 * 1024;
    /// A later piece is the bytes written so far divided by this, so the memory allocated ahead of the data is about this
    /// fraction of it.
    static constexpr size_t PIECE_SIZE_DIVISOR = 8;

    void nextImpl() override;
    std::unique_ptr<ReadBuffer> getReadBufferImpl() override;
    void addPiece(size_t size);
    VectorWithMemoryTracking<std::string_view> getWrittenPieces() const;

    VectorWithMemoryTracking<PODArray<char>> pieces;
    /// Of each piece but the last, whose written bytes end at `pos`.
    VectorWithMemoryTracking<size_t> pieces_written;
    size_t allocated_bytes = 0;
};

}
