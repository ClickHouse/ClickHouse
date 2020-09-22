#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <common/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_)), buf(in)
{
    if (header_.columns() > 1 || header_.getDataTypes()[0]->getTypeId() != TypeIndex::String)
    {
        throw Exception("This input format is only suitable for tables with a single column of type String.", ErrorCodes::INCORRECT_QUERY);
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
    bool newline = true;
    bool over = false;

    char * pos;

    while (newline)
    {
        pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());
        buf.position() = pos;
        if (buf.position() == buf.buffer().end())
        {
            over = true;
            break;
        }
        else if (*buf.position() == '\n')
        {
            newline = false;
        }
        else if (*buf.position() == '\\')
        {
            ++buf.position();
            if (!buf.eof())
                ++buf.position();
        }
    }

    buf.makeContinuousMemoryFromCheckpointToPos();
    char * end = over ? buf.position(): ++buf.position();
    buf.rollbackToCheckpoint();
    column.insertData(buf.position(), end - (over ? 0 : 1) - buf.position());
    buf.position() = end;
}

bool LineAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (!buf.eof())
        readLineObject(*columns[0]);

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
