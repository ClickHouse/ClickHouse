#include <IO/ReadHelpers.h>
#include <common/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    skipWhitespaceIfAny(in);

    char * pos = in.position();
    size_t balance = 0;
    bool quotes = false;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && (balance || memory.size() + static_cast<size_t>(pos - in.position()) < min_chunk_size))
    {
        const auto current_object_size = memory.size() + static_cast<size_t>(pos - in.position());
        if (current_object_size > 10 * min_chunk_size)
            throw ParsingException("Size of JSON object is extremely large. Expected not greater than " +
            std::to_string(min_chunk_size) + " bytes, but current is " + std::to_string(current_object_size) +
            " bytes per row. Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, most likely JSON is malformed", ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
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

            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;

            else if (*pos == '{')
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

            if (balance == 0)
                ++number_of_rows;
        }
    }

    saveUpToPosition(in, memory, pos);
    return {loadAtPosition(in, memory, pos), number_of_rows};
}

}
