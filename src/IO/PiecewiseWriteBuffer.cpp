#include <IO/PiecewiseWriteBuffer.h>

#include <algorithm>

namespace DB
{

namespace
{

/// Reads pieces of memory in order. With the memory given, it owns it and frees each piece once it is read.
class ReadBufferFromPieces : public ReadBuffer
{
public:
    explicit ReadBufferFromPieces(VectorWithMemoryTracking<std::string_view> pieces_, VectorWithMemoryTracking<PODArray<char>> memory_ = {})
        : ReadBuffer(nullptr, 0)
        , pieces(std::move(pieces_))
        , memory(std::move(memory_))
    {
    }

private:
    bool nextImpl() override
    {
        for (; next_piece != pieces.size(); ++next_piece)
        {
            if (next_piece != 0 && !memory.empty())
                PODArray<char>{}.swap(memory[next_piece - 1]);
            if (!pieces[next_piece].empty())
                break;
        }
        if (next_piece == pieces.size())
            return false;

        const auto & piece = pieces[next_piece];
        /// Only read through: the buffer's position type is not const. ReadBuffer::set would leave the piece unread.
        BufferBase::set(const_cast<char *>(piece.data()), piece.size(), 0);
        ++next_piece;
        return true;
    }

    VectorWithMemoryTracking<std::string_view> pieces;
    VectorWithMemoryTracking<PODArray<char>> memory;
    size_t next_piece = 0;
};

}

PiecewiseWriteBuffer::PiecewiseWriteBuffer() : WriteBuffer(nullptr, 0)
{
    addPiece(INITIAL_PIECE_SIZE);
}

PiecewiseWriteBuffer::~PiecewiseWriteBuffer()
{
    if (!isFinalized() && !isCanceled())
        cancel();
}

std::unique_ptr<ReadBuffer> PiecewiseWriteBuffer::readWritten() const
{
    return std::make_unique<ReadBufferFromPieces>(getWrittenPieces());
}

void PiecewiseWriteBuffer::clear()
{
    pieces.clear();
    pieces_written.clear();
    allocated_bytes = 0;
    bytes = 0;
    addPiece(INITIAL_PIECE_SIZE);
}

size_t PiecewiseWriteBuffer::allocatedBytes() const
{
    return allocated_bytes + pieces.capacity() * sizeof(PODArray<char>) + pieces_written.capacity() * sizeof(size_t);
}

void PiecewiseWriteBuffer::nextImpl()
{
    auto & piece = pieces.back();
    size_t written = pos - piece.data();
    /// An explicit next() with room left in the piece goes on in it.
    if (written < piece.size())
    {
        set(pos, piece.size() - written);
        return;
    }

    /// The first piece grows in place up to MIN_PIECE_SIZE, so that a small buffer is one piece; the reallocation may
    /// move the bytes written.
    if (pieces.size() == 1 && piece.size() < MIN_PIECE_SIZE)
    {
        piece.reserve(piece.size() * 2);
        piece.resize(piece.capacity());
        allocated_bytes = piece.allocated_bytes();
        set(piece.data() + written, piece.size() - written);
        return;
    }

    pieces_written.back() = written;
    addPiece(std::clamp(count() / PIECE_SIZE_DIVISOR, MIN_PIECE_SIZE, MAX_PIECE_SIZE));
}

std::unique_ptr<ReadBuffer> PiecewiseWriteBuffer::getReadBufferImpl()
{
    auto written_pieces = getWrittenPieces();
    auto read_buffer = std::make_unique<ReadBufferFromPieces>(std::move(written_pieces), std::move(pieces));
    pieces.clear();
    pieces_written.clear();
    allocated_bytes = 0;
    set(nullptr, 0);
    cancel();
    return read_buffer;
}

void PiecewiseWriteBuffer::addPiece(size_t size)
{
    auto & piece = pieces.emplace_back();
    /// The allocation is rounded up to a power of two; the piece takes all of it.
    piece.reserve(size);
    piece.resize(piece.capacity());
    pieces_written.push_back(0);
    allocated_bytes += piece.allocated_bytes();
    set(piece.data(), piece.size());
}

VectorWithMemoryTracking<std::string_view> PiecewiseWriteBuffer::getWrittenPieces() const
{
    VectorWithMemoryTracking<std::string_view> result;
    result.reserve(pieces.size());
    for (size_t i = 0; i + 1 < pieces.size(); ++i)
        result.emplace_back(pieces[i].data(), pieces_written[i]);
    result.emplace_back(pieces.back().data(), pos - pieces.back().data());
    return result;
}

}
