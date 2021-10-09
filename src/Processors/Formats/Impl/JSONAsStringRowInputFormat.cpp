#include <Processors/Formats/Impl/JSONAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <common/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_)), buf(in)
{
    if (header_.columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the number of columns is {}",
            header_.columns());

    if (!isString(removeNullable(removeLowCardinality(header_.getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the column type is {}",
            header_.getByPosition(0).type->getName());
}

void JSONAsStringRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf.reset();
}

void JSONAsStringRowInputFormat::readJSONObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    size_t balance = 0;
    bool quotes = false;

    if (*buf.position() != '{')
        throw Exception("JSON object must begin with '{'.", ErrorCodes::INCORRECT_DATA);

    ++buf.position();
    ++balance;

    char * pos;

    while (balance)
    {
        if (buf.eof())
            throw Exception("Unexpected end of file while parsing JSON object.", ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'"', '\\'>(buf.position(), buf.buffer().end());
            buf.position() = pos;
            if (buf.position() == buf.buffer().end())
                continue;
            if (*buf.position() == '"')
            {
                quotes = false;
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
        else
        {
            pos = find_first_symbols<'"', '{', '}', '\\'>(buf.position(), buf.buffer().end());
            buf.position() = pos;
            if (buf.position() == buf.buffer().end())
                continue;
            if (*buf.position() == '{')
            {
                ++balance;
                ++buf.position();
            }
            else if (*buf.position() == '}')
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
            else if (*buf.position() == '"')
            {
                quotes = true;
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

bool JSONAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    skipWhitespaceIfAny(buf);

    if (!buf.eof())
        readJSONObject(*columns[0]);

    skipWhitespaceIfAny(buf);
    if (!buf.eof() && *buf.position() == ',')
        ++buf.position();
    skipWhitespaceIfAny(buf);

    return !buf.eof();
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
