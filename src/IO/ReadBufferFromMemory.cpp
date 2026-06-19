#include <IO/ReadBufferFromMemory.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

template <typename Derived>
off_t ReadBufferFromMemoryHelper<Derived>::seekImpl(off_t offset, int whence)
{
    auto & derived = static_cast<Derived &>(*this);
    auto & pos = derived.pos;
    auto & internal_buffer = derived.internal_buffer;
    auto & working_buffer = derived.working_buffer;

    if (whence == SEEK_SET)
    {
        if (offset >= 0 && internal_buffer.begin() + offset <= internal_buffer.end())
        {
            pos = internal_buffer.begin() + offset;
            working_buffer = internal_buffer; /// We need to restore `working_buffer` in case the position was at EOF before this seek().
            return static_cast<size_t>(pos - internal_buffer.begin());
        }
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position is out of bounds. Offset: {}, Max: {}",
            offset,
            static_cast<size_t>(internal_buffer.end() - internal_buffer.begin()));
    }
    if (whence == SEEK_CUR)
    {
        BufferBase::Position new_pos = pos + offset;
        if (new_pos >= internal_buffer.begin() && new_pos <= internal_buffer.end())
        {
            pos = new_pos;
            working_buffer = internal_buffer; /// We need to restore `working_buffer` in case the position was at EOF before this seek().
            return static_cast<size_t>(pos - internal_buffer.begin());
        }
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position is out of bounds. Offset: {}, Max: {}",
            offset,
            static_cast<size_t>(internal_buffer.end() - internal_buffer.begin()));
    }
    if (whence == SEEK_END)
    {
        BufferBase::Position new_pos = internal_buffer.end() + offset;
        if (new_pos >= internal_buffer.begin() && new_pos <= internal_buffer.end())
        {
            pos = new_pos;
            working_buffer = internal_buffer;
            return static_cast<size_t>(pos - internal_buffer.begin());
        }
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position is out of bounds. Offset: {}, Max: {}",
            offset,
            static_cast<size_t>(internal_buffer.end() - internal_buffer.begin()));
    }
    throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET, SEEK_CUR and SEEK_END seek modes allowed.");
}

template <typename Derived>
off_t ReadBufferFromMemoryHelper<Derived>::getPositionImpl()
{
    auto & derived = static_cast<Derived &>(*this);
    return derived.pos - derived.internal_buffer.begin();
}

std::unique_ptr<ReadBufferFromMemory> ReadBufferFromMemory::getView(size_t offset, std::optional<size_t> size)
{
    const size_t buf_size = static_cast<size_t>(internal_buffer.end() - internal_buffer.begin());
    if (offset > buf_size)
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "getView offset {} exceeds buffer size {}",
            offset,
            buf_size);
    const size_t view_size = size.value_or(buf_size - offset);
    if (view_size > buf_size - offset)
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "getView size {} exceeds remaining buffer {} at offset {}",
            view_size,
            buf_size - offset,
            offset);
    return std::make_unique<ReadBufferFromMemory>(internal_buffer.begin() + offset, view_size);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ReadBufferFromMemoryHelper<ReadBufferFromMemory>;
template class ReadBufferFromMemoryHelper<ReadBufferFromMemoryFileBase>;

ReadBufferFromMemoryFileBase::ReadBufferFromMemoryFileBase(bool owns_memory,
    String file_name_,
    std::string_view data)
    : ReadBufferFromFileBase(
        data.size() /*buf_size*/,
        owns_memory
            ? nullptr
            : const_cast<char *>(data.data()) /*existing_memory*/,
        0 /*alignment*/,
        data.size() /*file_size*/)
    , file_name(std::move(file_name_))
{
    chassert(data.size() == internal_buffer.size());

    if (owns_memory)
        std::memcpy(internal_buffer.begin(), data.data(), data.size());

    working_buffer = internal_buffer;
}

}
