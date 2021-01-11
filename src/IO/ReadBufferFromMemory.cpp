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
        if (offset >= 0 && working_buffer.begin() + offset < working_buffer.end())
        {
            pos = working_buffer.begin() + offset;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else if (whence == SEEK_CUR)
    {
        Position new_pos = pos + offset;
        if (new_pos >= working_buffer.begin() && new_pos < working_buffer.end())
        {
            pos = new_pos;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else
        throw Exception("Only SEEK_SET and SEEK_CUR seek modes allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

off_t ReadBufferFromMemory::getPosition()
{
    return pos - working_buffer.begin();
}

}
