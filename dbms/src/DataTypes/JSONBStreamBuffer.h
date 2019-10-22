#pragma once

#include "../IO/ReadHelpers.h"
#include "../IO/WriteHelpers.h"
#include "../Formats/FormatSettings.h"
#include "JSONBStreamFactory.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_JSON;
    extern const int CANNOT_PARSE_QUOTED_STRING;
}

template <FormatStyle format>
struct JSONBStreamBuffer
{
public:
    typedef char Ch;

    JSONBStreamBuffer(ReadBuffer * read_buffer_, WriteBuffer * write_buffer_, const FormatSettings & settings_)
        : read_buffer(read_buffer_), write_buffer(write_buffer_), settings(settings_)
    {
    }

    JSONBStreamBuffer(ReadBuffer * read_buffer_, const FormatSettings & settings_) : JSONBStreamBuffer(read_buffer_, nullptr, settings_) {}

    JSONBStreamBuffer(WriteBuffer * write_buffer_, const FormatSettings & settings_) : JSONBStreamBuffer(nullptr, write_buffer_, settings_) {}

    void Flush() {  /* do nothing */ }

    char Take()
    {
        if (read_buffer->eof())
            return char(0);

        return *read_buffer->position()++;
    }

    char Peek() const
    {
        if (read_buffer->eof())
            return char(0);

        return *read_buffer->position();
    }

    size_t Tell() const
    {
        return read_buffer->count();
    }

    void Put(char value)
    {
        write_buffer->nextIfAtEnd();
        *write_buffer->position() = value;
        ++write_buffer->position();
    }

    void skipQuoted()
    {
        if constexpr (format == FormatStyle::CSV)
        {
            if (read_buffer->eof())
                throwReadAfterEOF();

            const char delimiter = settings.csv.delimiter;
            const char maybe_quote = *read_buffer->position();

            if (maybe_quote == delimiter)
                throw Exception("Cannot parse empty string to JSON Type", ErrorCodes::CANNOT_PARSE_JSON);

            if ((settings.csv.allow_single_quotes && maybe_quote == '\'') || (settings.csv.allow_double_quotes && maybe_quote == '"'))
            {
                ++read_buffer->position();

                if (quote_char && quote_char != maybe_quote)
                    throw Exception("Cannot parse CSV string: expected closing quote " + toString(quote_char), ErrorCodes::CANNOT_PARSE_QUOTED_STRING);

                quote_char = (quote_char ? char(0) : maybe_quote);
            }
        }
        else if constexpr (format == FormatStyle::QUOTED)
        {
            if (read_buffer->eof() || *read_buffer->position() != '\'')
                throw Exception("Cannot parse quoted string: expected " + (quote_char ? String("closing") : String("opening")) + " quote",
                                ErrorCodes::CANNOT_PARSE_QUOTED_STRING);

            ++read_buffer->position();
            quote_char = (quote_char == '\'' ? char(0) : char('\''));
        }
    }

    char * PutBegin() { throw Exception("Method PutBegin is not supported for JSONBStreamBuffer", ErrorCodes::NOT_IMPLEMENTED); }

    size_t PutEnd(char * /*value*/) { throw Exception("Method PutEnd is not supported for JSONBStreamBuffer", ErrorCodes::NOT_IMPLEMENTED); }

private:
    char quote_char{0};
    ReadBuffer * read_buffer;
    WriteBuffer * write_buffer;
    const FormatSettings & settings;
};

}
