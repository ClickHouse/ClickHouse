#include <Processors/Formats/Impl/JSONAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

JSONAsRowInputFormat::JSONAsRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : JSONAsRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_) {}

JSONAsRowInputFormat::JSONAsRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, Params params_) :
    IRowInputFormat(header_, *buf_, std::move(params_)), buf(std::move(buf_))
{
    if (header_.columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String or Object, but the number of columns is {}",
            header_.columns());
}

void JSONAsRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf->reset();
}

void JSONAsRowInputFormat::readPrefix()
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(*buf);

    skipWhitespaceIfAny(*buf);
    if (!buf->eof() && *buf->position() == '[')
    {
        ++buf->position();
        data_in_square_brackets = true;
    }
}

void JSONAsRowInputFormat::readSuffix()
{
    skipWhitespaceIfAny(*buf);
    if (data_in_square_brackets)
    {
        assertChar(']', *buf);
        skipWhitespaceIfAny(*buf);
        data_in_square_brackets = false;
    }
    if (!buf->eof() && *buf->position() == ';')
    {
        ++buf->position();
        skipWhitespaceIfAny(*buf);
    }
    assertEOF(*buf);
}

bool JSONAsRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    assert(columns.size() == 1);
    assert(serializations.size() == 1);

    if (!allow_new_rows)
        return false;

    skipWhitespaceIfAny(*buf);
    if (!buf->eof())
    {
        if (!data_in_square_brackets && *buf->position() == ';')
        {
            /// ';' means the end of query, but it cannot be before ']'.
            return allow_new_rows = false;
        }
        else if (data_in_square_brackets && *buf->position() == ']')
        {
            /// ']' means the end of query.
            return allow_new_rows = false;
        }
    }

    if (!buf->eof())
        readJSONObject(*columns[0]);

    skipWhitespaceIfAny(*buf);
    if (!buf->eof() && *buf->position() == ',')
        ++buf->position();
    skipWhitespaceIfAny(*buf);

    return !buf->eof();
}

void JSONAsRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    IInputFormat::setReadBuffer(*buf);
}


JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(
    const Block & header_, ReadBuffer & in_, Params params_)
    : JSONAsRowInputFormat(header_, in_, params_)
{
    if (!isString(removeNullable(removeLowCardinality(header_.getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the column type is {}",
            header_.getByPosition(0).type->getName());
}

void JSONAsStringRowInputFormat::readJSONObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    size_t balance = 0;
    bool quotes = false;

    if (*buf->position() != '{')
        throw Exception("JSON object must begin with '{'.", ErrorCodes::INCORRECT_DATA);

    ++buf->position();
    ++balance;

    char * pos;

    while (balance)
    {
        if (buf->eof())
            throw Exception("Unexpected end of file while parsing JSON object.", ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'"', '\\'>(buf->position(), buf->buffer().end());
            buf->position() = pos;
            if (buf->position() == buf->buffer().end())
                continue;
            if (*buf->position() == '"')
            {
                quotes = false;
                ++buf->position();
            }
            else if (*buf->position() == '\\')
            {
                ++buf->position();
                if (!buf->eof())
                {
                    ++buf->position();
                }
            }
        }
        else
        {
            pos = find_first_symbols<'"', '{', '}', '\\'>(buf->position(), buf->buffer().end());
            buf->position() = pos;
            if (buf->position() == buf->buffer().end())
                continue;
            if (*buf->position() == '{')
            {
                ++balance;
                ++buf->position();
            }
            else if (*buf->position() == '}')
            {
                --balance;
                ++buf->position();
            }
            else if (*buf->position() == '\\')
            {
                ++buf->position();
                if (!buf->eof())
                {
                    ++buf->position();
                }
            }
            else if (*buf->position() == '"')
            {
                quotes = true;
                ++buf->position();
            }
        }
    }
    buf->makeContinuousMemoryFromCheckpointToPos();
    char * end = buf->position();
    buf->rollbackToCheckpoint();
    column.insertData(buf->position(), end - buf->position());
    buf->position() = end;
}


JSONAsObjectRowInputFormat::JSONAsObjectRowInputFormat(
    const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : JSONAsRowInputFormat(header_, in_, params_)
    , format_settings(format_settings_)
{
    if (!isObject(header_.getByPosition(0).type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Input format JSONAsObject is only suitable for tables with a single column of type Object but the column type is {}",
            header_.getByPosition(0).type->getName());
}

void JSONAsObjectRowInputFormat::readJSONObject(IColumn & column)
{
    serializations[0]->deserializeTextJSON(column, *buf, format_settings);
}

void registerInputFormatJSONAsString(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsString", [](
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
    factory.registerFileSegmentationEngine("JSONAsString", &fileSegmentationEngineJSONEachRow);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsString", nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerJSONAsStringSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("JSONAsString", [](const FormatSettings &)
    {
        return std::make_shared<JSONAsStringExternalSchemaReader>();
    });
}

void registerInputFormatJSONAsObject(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsObject", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<JSONAsObjectRowInputFormat>(sample, buf, std::move(params), settings);
    });
}

void registerNonTrivialPrefixAndSuffixCheckerJSONAsObject(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsObject", nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsObject", &fileSegmentationEngineJSONEachRow);
}

void registerJSONAsObjectSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("JSONAsObject", [](const FormatSettings &)
    {
        return std::make_shared<JSONAsObjectExternalSchemaReader>();
    });
}

}
