#include <Server/HTTP/ReadHeaders.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <Poco/Net/NetException.h>

namespace DB
{

void readHeaders(
    Poco::Net::MessageHeader & headers,
    ReadBuffer & in,
    size_t max_fields_number,
    size_t max_name_length,
    size_t max_value_length,
    size_t max_total_size)
{
    char ch = 0;  // silence uninitialized warning from gcc-*
    std::string name;
    std::string value;

    name.reserve(32);
    value.reserve(64);

    size_t fields = 0;
    size_t total_size = 0;

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

        total_size += name.size() + value.size();
        if (max_total_size && total_size > max_total_size)
            throw Poco::Net::MessageException("Total request header size is too large");

        skipToNextLineOrEOF(in);

        Poco::trimRightInPlace(value);
        std::string decoded_value = Poco::Net::MessageHeader::decodeWord(value);
        /// RFC 2047 decoding can produce bytes that are forbidden in a field value (RFC 9110, section 5.5),
        /// e.g. `=0D=0A` would become a line break that splits the header when it is written out again.
        if (decoded_value.find_first_of(std::string_view("\r\n\0", 3)) != std::string::npos)
            throw Poco::Net::MessageException(fmt::format("Value of field \"{}\" contains CR, LF or NUL", name));
        headers.add(name, decoded_value);
        ++fields;
    }
}

}
