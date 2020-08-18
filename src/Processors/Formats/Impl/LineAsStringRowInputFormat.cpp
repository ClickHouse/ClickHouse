#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <common/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_)), buf(in)
{
    if (header_.columns() > 1 || header_.getDataTypes()[0]->getTypeId() != TypeIndex::String)
    {
        throw Exception("This input format is only suitable for tables with a single column of type String.", ErrorCodes::LOGICAL_ERROR);
    }
}

void LineAsStringRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf.reset();
}

void LineAsStringRowInputFormat::readLineObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    size_t balance = 0;

    if (*buf.position() == ';') {
        ++buf.position();
        if(buf.eof())
            return;
    }

    if (*buf.position() != '"')
        throw Exception("Line object must begin with '\"'.", ErrorCodes::INCORRECT_DATA);

    ++buf.position();
    ++balance;

    char * pos;

    while (balance)
    {
        if (buf.eof())
            throw Exception("Unexpected end of file while parsing Line object.", ErrorCodes::INCORRECT_DATA);

        pos = find_last_symbols_or_null<'"', '\\'>(buf.position(), buf.buffer().end());
        buf.position() = pos;
        if (buf.position() == buf.buffer().end())
            continue;
        else if (*buf.position() == '"')
        {
            --balance;
            ++buf.position();
        }
        else if (*buf.position() == '\\')
            {
            ++buf.position();
            if (!buf.eof())
            {
            	++buf.position();
            }
        }
        
    }
    buf.makeContinuousMemoryFromCheckpointToPos();
    char * end = buf.position();
    buf.rollbackToCheckpoint();
    column.insertData(buf.position(), end - buf.position());
    buf.position() = end;
}

bool LineAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    skipWhitespaceIfAny(buf);

    if (!buf.eof())
        readLineObject(*columns[0]);

    skipWhitespaceIfAny(buf);
    if (!buf.eof() && *buf.position() == ',')
        ++buf.position();
    skipWhitespaceIfAny(buf);

    return !buf.eof();
}

void registerInputFormatProcessorLineAsString(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("LineAsString", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<LineAsStringRowInputFormat>(sample, buf, params);
    });
}

}
