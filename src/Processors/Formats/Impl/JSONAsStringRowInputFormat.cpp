#include <Processors/Formats/Impl/JSONAsStringRowInputFormat.h>
#include <Formats/JSONUtils.h>
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
    extern const int ILLEGAL_COLUMN;
}

JSONAsRowInputFormat::JSONAsRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_) :
    JSONEachRowRowInputFormat(in_, header_, std::move(params_), format_settings_, false)
{
}

bool JSONAsRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & extension)
{
    if (!allow_new_rows)
        return false;

    skipWhitespaceIfAny(*in);
    if (!in->eof())
    {
        if (!data_in_square_brackets && *in->position() == ';')
        {
            /// ';' means the end of query, but it cannot be before ']'.
            return allow_new_rows = false;
        }
        else if (data_in_square_brackets && *in->position() == ']')
        {
            /// ']' means the end of query.
            return allow_new_rows = false;
        }
    }

    if (!in->eof())
    {
        readJSONObject(*columns[json_column_index]);
        extension.read_columns.resize(columns.size(), true);
        for (size_t i = 0; i != columns.size(); ++i)
        {
            if (i != json_column_index)
            {
                columns[i]->insertDefault();
                extension.read_columns[i] = false;
            }
        }
    }

    skipWhitespaceIfAny(*in);
    if (!in->eof() && *in->position() == ',')
        ++in->position();
    skipWhitespaceIfAny(*in);

    return !in->eof();
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(
    const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : JSONAsStringRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, format_settings_)
{
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(
    const DB::Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, DB::IRowInputFormat::Params params_, const DB::FormatSettings & format_settings_)
    : JSONAsRowInputFormat(header_, *buf_, params_, format_settings_), buf(std::move(buf_))
{
    auto header_types = header_.getDataTypes();
    size_t i = 0;
    for (; i != header_types.size(); ++i)
    {
        if (isString(removeNullable(removeLowCardinality(header_types[i]))))
            break;
    }

    if (i == header_types.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSONAsString input format is only suitable for tables with a column of type String but there are no such column");

    json_column_index = i;
}

void JSONAsStringRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    JSONAsRowInputFormat::setReadBuffer(*buf);
}

void JSONAsStringRowInputFormat::resetReadBuffer()
{
    buf.reset();
    JSONAsRowInputFormat::resetReadBuffer();
}

void JSONAsStringRowInputFormat::readJSONObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    size_t balance = 0;
    bool quotes = false;

    if (*buf->position() != '{')
        throw Exception(ErrorCodes::INCORRECT_DATA, "JSON object must begin with '{{'.");

    ++buf->position();
    ++balance;

    char * pos;

    while (balance)
    {
        if (buf->eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of file while parsing JSON object.");

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
    : JSONAsRowInputFormat(header_, in_, params_, format_settings_)
{
    auto header_types = header_.getDataTypes();
    size_t i = 0;
    for (; i != header_types.size(); ++i)
    {
        if (isObject(header_types[i]))
            break;
    }

    if (i == header_types.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input format JSONAsObject is only suitable for tables with a column of type Object but there are no such columns");

    json_column_index = i;
}

void JSONAsObjectRowInputFormat::readJSONObject(IColumn & column)
{
    serializations[json_column_index]->deserializeTextJSON(column, *in, format_settings);
}

JSONAsObjectExternalSchemaReader::JSONAsObjectExternalSchemaReader(const FormatSettings & settings)
{
    if (!settings.json.allow_object_type)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Cannot infer the data structure in JSONAsObject format because experimental Object type is not allowed. Set setting "
            "allow_experimental_object_type = 1 in order to allow it");
}

void registerInputFormatJSONAsString(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsString", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONAsStringRowInputFormat>(sample, buf, params, format_settings);
    });
}

void registerFileSegmentationEngineJSONAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsString", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsString", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
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
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsObject", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsObject", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerJSONAsObjectSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("JSONAsObject", [](const FormatSettings & settings)
    {
        return std::make_shared<JSONAsObjectExternalSchemaReader>(settings);
    });
}

}
