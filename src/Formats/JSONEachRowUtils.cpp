#include <IO/ReadHelpers.h>
#include <common/find_symbols.h>

namespace DB
{

bool fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    skipWhitespaceIfAny(in);

    char * pos = in.position();
    size_t balance = 0;
    bool quotes = false;

    while (loadAtPosition(in, memory, pos) && (balance || memory.size() + static_cast<size_t>(pos - in.position()) < min_chunk_size))
    {
        if (quotes)
        {
            pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());
            if (pos == in.buffer().end())
                continue;
            if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                ++pos;
                quotes = false;
            }
        }
        else
        {
            pos = find_first_symbols<'{', '}', '\\', '"'>(pos, in.buffer().end());
            if (pos == in.buffer().end())
                continue;
            if (*pos == '{')
            {
                ++balance;
                ++pos;
            }
            else if (*pos == '}')
            {
                --balance;
                ++pos;
            }
            else if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                quotes = true;
                ++pos;
            }
        }
    }

    saveUpToPosition(in, memory, pos);
    return loadAtPosition(in, memory, pos);
}

}
