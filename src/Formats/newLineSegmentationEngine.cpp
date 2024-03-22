#include <Formats/newLineSegmentationEngine.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::pair<bool, size_t> newLineFileSegmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\r', '\n'>(pos, in.buffer().end());
        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        else if (pos == in.buffer().end())
            continue;

        ++number_of_rows;
        if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows))
            need_more_data = false;

        if (*pos == '\n')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos) && *pos == '\r')
                ++pos;
        }
        else if (*pos == '\r')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos) && *pos == '\n')
                ++pos;
        }
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

}
