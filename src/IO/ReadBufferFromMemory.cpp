#include <IO/ReadBufferFromMemory.h>

#include <Common/Exception.h>
#include <base/types.h>

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

    const size_t buffer_size = static_cast<size_t>(internal_buffer.end() - internal_buffer.begin());

    if (whence == SEEK_SET)
    {
        if (offset >= 0 && static_cast<UInt64>(offset) <= buffer_size)
        {
            pos = internal_buffer.begin() + offset;
            working_buffer = internal_buffer; /// We need to restore `working_buffer` in case the position was at EOF before this seek().
            return offset;
        }
        throw Exception(
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position is out of bounds. Offset: {}, Max: {}",
            offset,
            buffer_size);
    }
    if (whence == SEEK_CUR)
    {
        const off_t cur = static_cast<off_t>(pos - internal_buffer.begin());
        if (offset < -cur || offset > static_cast<off_t>(buffer_size) - cur)
            throw Exception(
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}, Max: {}", offset, buffer_size);
        pos = internal_buffer.begin() + (cur + offset);
        working_buffer = internal_buffer;
        return cur + offset;
    }
    if (whence == SEEK_END)
    {
        if (offset > 0)
            throw Exception(
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}, Max: {}", offset, buffer_size);
        const size_t back = static_cast<size_t>(-static_cast<UInt64>(offset)); /// |offset|, overflow-safe incl. INT64_MIN
        if (back > buffer_size)
            throw Exception(
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}, Max: {}", offset, buffer_size);
        pos = internal_buffer.end() - back;
        working_buffer = internal_buffer;
        return static_cast<off_t>(buffer_size - back);
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
