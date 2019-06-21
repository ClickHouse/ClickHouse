#include <DataTypes/SmallestJSON/BufferSmallestJSONStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_JSON;
    extern const int CANNOT_PARSE_QUOTED_STRING;
}

template<typename BufferType>
char BufferStreamHelper::Take(BufferType & /*buffer*/)
{
    throw Exception("Method Take is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
}

template<typename BufferType>
char BufferStreamHelper::Peek(BufferType & /*buffer*/)
{
    throw Exception("Method Peek is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
}

template<typename BufferType>
size_t BufferStreamHelper::Tell(BufferType & /*buffer*/)
{
    throw Exception("Method Tell is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
}

template<typename BufferType>
void BufferStreamHelper::Put(BufferType & /*buffer*/, char /*value*/)
{
    throw Exception("Method Put is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
}

template<RapidFormat format>
char BufferStreamHelper::SkipQuoted(WriteBuffer & /*buffer*/, const FormatSettings & /*setting*/, char /*maybe_opening_quoted*/)
{
    throw Exception("Method SkipQuoted is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
}

template<RapidFormat format>
char BufferStreamHelper::SkipQuoted(ReadBuffer & /*buffer*/, const FormatSettings & /*setting*/, char /*maybe_opening_quoted*/)
{
    /// By default, we don't need to skip any quoted characters
    return 0;
}

template<>
char BufferStreamHelper::Take(ReadBuffer & buffer)
{
    if (buffer.eof())
        return char(0);

    return *buffer.position()++;
}


template<>
char BufferStreamHelper::Peek(ReadBuffer & buffer)
{
    if (buffer.eof())
        return char(0);

    return *buffer.position();
}

template<>
void BufferStreamHelper::Put(WriteBuffer & buffer, char value)
{
    buffer.nextIfAtEnd();
    *buffer.position() = value;
    ++buffer.position();
}

template<>
size_t BufferStreamHelper::Tell(ReadBuffer & buffer)
{
    return buffer.count();
}

template<>
char BufferStreamHelper::SkipQuoted<RapidFormat::CSV>(ReadBuffer & buffer, const FormatSettings & setting, char maybe_opening_quoted)
{
    if (buffer.eof())
        throwReadAfterEOF();

    const char delimiter = setting.csv.delimiter;
    const char maybe_quote = *buffer.position();

    if (maybe_quote == delimiter)
        throw Exception("Cannot parse empty string to JSON Type", ErrorCodes::CANNOT_PARSE_JSON);

    if ((setting.csv.allow_single_quotes && maybe_quote == '\'') || (setting.csv.allow_double_quotes && maybe_quote == '"'))
    {
        ++buffer.position();

        if (!maybe_opening_quoted && maybe_opening_quoted != maybe_quote)
            throw Exception("Cannot parse CSV string: expected closing quote " + toString(maybe_opening_quoted), ErrorCodes::CANNOT_PARSE_QUOTED_STRING);

        return maybe_opening_quoted ? char(0) : maybe_quote;
    }

    return char(0);
}

template<>
char BufferStreamHelper::SkipQuoted<RapidFormat::QUOTED>(ReadBuffer & buffer, const FormatSettings & /*setting*/, char maybe_opening_quoted)
{
    if (buffer.eof() || *buffer.position() != '\'')
        throw Exception("Cannot parse quoted string: expected " + (maybe_opening_quoted ? String("closing") : String("opening")) + " quote",
                        ErrorCodes::CANNOT_PARSE_QUOTED_STRING);

    ++buffer.position();
    return maybe_opening_quoted == '\'' ? char(0) : char('\'');
}

template char BufferStreamHelper::Peek(WriteBuffer & buffer);
template char BufferStreamHelper::Take(WriteBuffer & buffer);
template size_t BufferStreamHelper::Tell(WriteBuffer & buffer);
template void BufferStreamHelper::Put(ReadBuffer & buffer, char value);

template char BufferStreamHelper::SkipQuoted<RapidFormat::JSON>(ReadBuffer &, const FormatSettings &, char);
template char BufferStreamHelper::SkipQuoted<RapidFormat::ESCAPED>(ReadBuffer &, const FormatSettings &, char);

template char BufferStreamHelper::SkipQuoted<RapidFormat::CSV>(WriteBuffer &, const FormatSettings &, char);
template char BufferStreamHelper::SkipQuoted<RapidFormat::JSON>(WriteBuffer &, const FormatSettings &, char);
template char BufferStreamHelper::SkipQuoted<RapidFormat::QUOTED>(WriteBuffer &, const FormatSettings &, char);
template char BufferStreamHelper::SkipQuoted<RapidFormat::ESCAPED>(WriteBuffer &, const FormatSettings &, char);

}
