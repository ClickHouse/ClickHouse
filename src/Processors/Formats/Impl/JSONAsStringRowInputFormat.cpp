#include <Processors/Formats/Impl/JSONAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <common/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

extern bool fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_)), buf(in)
{
    if (header_.columns() > 1 || header_.getDataTypes()[0]->getTypeId() != TypeIndex::String)
    {
        throw Exception("This input format is only suitable for tables with a single column of type String.",  ErrorCodes::LOGICAL_ERROR);
    }
}

void JSONAsStringRowInputFormat::readJSONObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    size_t balance = 0;
    size_t object_size = 0;
    bool quotes = false;

    if (*buf.position() != '{')
        throw Exception("JSON object must begin with '{'.",  ErrorCodes::INCORRECT_DATA);

    ++buf.position();
    ++object_size;
    ++balance;

    char * pos;

    while (balance)
    {
        if (buf.eof())
            throw Exception("Unexpected end of file while parsing JSON object.",  ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'"', '\\'>(buf.position(), buf.buffer().end());
            object_size += pos - buf.position();
            buf.position() = pos;
            if (buf.position() == buf.buffer().end())
                continue;
            if (*buf.position() == '"')
            {
                quotes = false;
                ++buf.position();
                ++object_size;
            }
            else if (*buf.position() == '\\')
            {
                ++buf.position();
                ++object_size;
                if (!buf.eof())
                {
                    ++buf.position();
                    ++object_size;
                }
            }
        }
        else
        {
            pos = find_first_symbols<'"', '{', '}', '\\'>(buf.position(), buf.buffer().end());
            object_size += pos - buf.position();
            buf.position() = pos;
            if (buf.position() == buf.buffer().end())
                continue;
            if (*buf.position() == '{')
            {
                ++balance;
                ++buf.position();
                ++object_size;
            }
            else if (*buf.position() == '}')
            {
                --balance;
                ++buf.position();
                ++object_size;
            }
            else if (*buf.position() == '\\')
            {
                ++buf.position();
                ++object_size;
                if (!buf.eof())
                {
                    ++buf.position();
                    ++object_size;
                }
            }
            else if (*buf.position() == '"')
            {
                quotes = true;
                ++buf.position();
                ++object_size;
            }
        }
    }
    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();
    column.insertData(buf.position(), object_size);
    buf.position() += object_size;
}

bool JSONAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    skipWhitespaceIfAny(buf);

    if (!buf.eof())
        readJSONObject(*columns[0]);

    skipWhitespaceIfAny(buf);
    if (!buf.eof() && *buf.position() == ',')
        ++buf.position();
    skipWhitespaceIfAny(buf);

    if (buf.eof())
        return false;

    return true;
}

void registerInputFormatProcessorJSONAsString(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("JSONAsString", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<JSONAsStringRowInputFormat>(sample, buf, params);
    });
}

void registerFileSegmentationEngineJSONAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsString", &fileSegmentationEngineJSONEachRowImpl);
}

}
