#include "ReadBufferFromMemory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

off_t ReadBufferFromMemory::seek(off_t offset, int whence)
{
    if (whence == SEEK_SET)
    {
        if (offset >= 0 && internal_buffer.begin() + offset < internal_buffer.end())
        {
            pos = internal_buffer.begin() + offset;
            working_buffer = internal_buffer; /// We need to restore `working_buffer` in case the position was at EOF before this seek().
            return static_cast<size_t>(pos - internal_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(static_cast<size_t>(internal_buffer.end() - internal_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else if (whence == SEEK_CUR)
    {
        Position new_pos = pos + offset;
        if (new_pos >= internal_buffer.begin() && new_pos < internal_buffer.end())
        {
            pos = new_pos;
            working_buffer = internal_buffer; /// We need to restore `working_buffer` in case the position was at EOF before this seek().
            return static_cast<size_t>(pos - internal_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(static_cast<size_t>(internal_buffer.end() - internal_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else
        throw Exception("Only SEEK_SET and SEEK_CUR seek modes allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

off_t ReadBufferFromMemory::getPosition()
{
    return pos - internal_buffer.begin();
}

}
