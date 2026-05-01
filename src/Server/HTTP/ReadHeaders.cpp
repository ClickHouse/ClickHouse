#include <Server/HTTP/ReadHeaders.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <Poco/Net/NetException.h>

namespace DB
{

void readHeaders(
    Poco::Net::MessageHeader & headers, ReadBuffer & in, size_t max_fields_number, size_t max_name_length, size_t max_value_length)
{
    char ch = 0;  // silence uninitialized warning from gcc-*
    std::string name;
    std::string value;

    name.reserve(32);
    value.reserve(64);

    size_t fields = 0;

    while (true)
    {
        if (fields > max_fields_number)
            throw Poco::Net::MessageException("Too many header fields");

        name.clear();
        value.clear();

        /// Field name
        while (in.peek(ch) && ch != ':' && !Poco::Ascii::isSpace(ch) && name.size() <= max_name_length)
        {
            name += ch;
            in.ignore();
        }

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        if (name.empty())
        {
            if (ch == '\r')
                /// Start of the empty-line delimiter
                break;
            if (ch == ':')
                throw Poco::Net::MessageException("Field name is empty");
        }
        else
        {
            if (name.size() > max_name_length)
                throw Poco::Net::MessageException("Field name is too long");
            if (ch != ':')
                throw Poco::Net::MessageException(fmt::format("Field name is invalid or no colon found: \"{}\"", name));
        }

        in.ignore();

        skipWhitespaceIfAny(in, true);

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        /// Field value - folded values not supported.
        while (in.read(ch) && ch != '\r' && ch != '\n' && value.size() <= max_value_length)
            value += ch;

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        if (ch == '\n')
            throw Poco::Net::MessageException("No CRLF found");

        if (value.size() > max_value_length)
            throw Poco::Net::MessageException("Field value is too long");

        skipToNextLineOrEOF(in);

        Poco::trimRightInPlace(value);
        headers.add(name, Poco::Net::MessageHeader::decodeWord(value));
        ++fields;
    }
}

}
