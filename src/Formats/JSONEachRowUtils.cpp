#include <IO/ReadHelpers.h>
#include <Formats/JSONEachRowUtils.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/Serializations/SerializationNullable.h>

#include <base/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

template <const char opening_bracket, const char closing_bracket>
static std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
{
    skipWhitespaceIfAny(in);

    char * pos = in.position();
    size_t balance = 0;
    bool quotes = false;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && (balance || memory.size() + static_cast<size_t>(pos - in.position()) < min_chunk_size || number_of_rows < min_rows))
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
            pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(pos, in.buffer().end());

            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;

            else if (*pos == opening_bracket)
            {
                ++balance;
                ++pos;
            }
            else if (*pos == closing_bracket)
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

std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    return fileSegmentationEngineJSONEachRowImpl<'{', '}'>(in, memory, min_chunk_size, 1);
}

std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
{
    return fileSegmentationEngineJSONEachRowImpl<'[', ']'>(in, memory, min_chunk_size, min_rows);
}

bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf)
{
    /// For JSONEachRow we can safely skip whitespace characters
    skipWhitespaceIfAny(buf);
    return buf.eof() || *buf.position() == '[';
}

bool readFieldImpl(ReadBuffer & in, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name, const FormatSettings & format_settings, bool yield_strings)
{
    try
    {
        bool as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();

        if (yield_strings)
        {
            String str;
            readJSONString(str, in);

            ReadBufferFromString buf(str);

            if (as_nullable)
                return SerializationNullable::deserializeWholeTextImpl(column, buf, format_settings, serialization);

            serialization->deserializeWholeText(column, buf, format_settings);
            return true;
        }

        if (as_nullable)
            return SerializationNullable::deserializeTextJSONImpl(column, in, format_settings, serialization);

        serialization->deserializeTextJSON(column, in, format_settings);
        return true;
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of key " + column_name + ")");
        throw;
    }
}

}
