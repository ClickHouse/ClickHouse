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
            std::to_string(static_cast<size_t>(internal_buffer.end() - internal_buffer.begin())));
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
            std::to_string(static_cast<size_t>(internal_buffer.end() - internal_buffer.begin())));
    }
    throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET and SEEK_CUR seek modes allowed.");
}

template <typename Derived>
off_t ReadBufferFromMemoryHelper<Derived>::getPositionImpl()
{
    auto & derived = static_cast<Derived &>(*this);
    return derived.pos - derived.internal_buffer.begin();
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
