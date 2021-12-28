#include <Server/HTTP/HTMLForm.h>

#include <Core/Settings.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Server/HTTP/ReadHeaders.h>

#include <Poco/CountingStream.h>
#include <Poco/Net/MultipartReader.h>
#include <Poco/Net/MultipartWriter.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/NullPartHandler.h>
#include <Poco/NullStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/UTF8String.h>

#include <sstream>


namespace DB
{

namespace
{

class NullPartHandler : public HTMLForm::PartHandler
{
public:
    void handlePart(const Poco::Net::MessageHeader &, ReadBuffer &) override {}
};

}

const std::string HTMLForm::ENCODING_URL = "application/x-www-form-urlencoded";
const std::string HTMLForm::ENCODING_MULTIPART = "multipart/form-data";
const int HTMLForm::UNKNOWN_CONTENT_LENGTH = -1;


HTMLForm::HTMLForm(const Settings & settings)
    : max_fields_number(settings.http_max_fields)
    , max_field_name_size(settings.http_max_field_name_size)
    , max_field_value_size(settings.http_max_field_value_size)
    , encoding(ENCODING_URL)
{
}


HTMLForm::HTMLForm(const Settings & settings, const std::string & encoding_) : HTMLForm(settings)
{
    encoding = encoding_;
}


HTMLForm::HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler)
    : HTMLForm(settings)
{
    load(request, requestBody, handler);
}


HTMLForm::HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody) : HTMLForm(settings)
{
    load(request, requestBody);
}


HTMLForm::HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request) : HTMLForm(settings, Poco::URI(request.getURI()))
{
}

HTMLForm::HTMLForm(const Settings & settings, const Poco::URI & uri) : HTMLForm(settings)
{
    ReadBufferFromString istr(uri.getRawQuery());  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    readQuery(istr);
}


void HTMLForm::load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler)
{
    clear();

    Poco::URI uri(request.getURI());
    const std::string & query = uri.getRawQuery();
    if (!query.empty())
    {
        ReadBufferFromString istr(query);
        readQuery(istr);
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST || request.getMethod() == Poco::Net::HTTPRequest::HTTP_PUT)
    {
        std::string media_type;
        NameValueCollection params;
        Poco::Net::MessageHeader::splitParameters(request.getContentType(), media_type, params);
        encoding = media_type;
        if (encoding == ENCODING_MULTIPART)
        {
            boundary = params["boundary"];
            readMultipart(requestBody, handler);
        }
        else
        {
            readQuery(requestBody);
        }
    }
}


void HTMLForm::load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody)
{
    NullPartHandler nah;
    load(request, requestBody, nah);
}


void HTMLForm::read(ReadBuffer & in)
{
    readQuery(in);
}


void HTMLForm::readQuery(ReadBuffer & in)
{
    size_t fields = 0;
    char ch = 0;  // silence "uninitialized" warning from gcc-*
    bool is_first = true;

    while (true)
    {
        if (max_fields_number > 0 && fields == max_fields_number)
            throw Poco::Net::HTMLFormException("Too many form fields");

        std::string name;
        std::string value;

        while (in.read(ch) && ch != '=' && ch != '&')
        {
            if (ch == '+')
                ch = ' ';
            if (name.size() < max_field_name_size)
                name += ch;
            else
                throw Poco::Net::HTMLFormException("Field name too long");
        }

        if (ch == '=')
        {
            while (in.read(ch) && ch != '&')
            {
                if (ch == '+')
                    ch = ' ';
                if (value.size() < max_field_value_size)
                    value += ch;
                else
                    throw Poco::Net::HTMLFormException("Field value too long");
            }
        }

        // Remove UTF-8 BOM from first name, if present
        if (is_first)
            Poco::UTF8::removeBOM(name);

        std::string decoded_name;
        std::string decoded_value;
        Poco::URI::decode(name, decoded_name);
        Poco::URI::decode(value, decoded_value);
        add(decoded_name, decoded_value);
        ++fields;

        is_first = false;

        if (in.eof())
            break;
    }
}


void HTMLForm::readMultipart(ReadBuffer & in_, PartHandler & handler)
{
    /// Assume there is always a boundary provided.
    assert(!boundary.empty());

    size_t fields = 0;
    MultipartReadBuffer in(in_, boundary);

    if (!in.skipToNextBoundary())
        throw Poco::Net::HTMLFormException("No boundary line found");

    /// Read each part until next boundary (or last boundary)
    while (!in.eof())
    {
        if (max_fields_number && fields > max_fields_number)
            throw Poco::Net::HTMLFormException("Too many form fields");

        Poco::Net::MessageHeader header;
        readHeaders(header, in, max_fields_number, max_field_name_size, max_field_value_size);
        skipToNextLineOrEOF(in);

        NameValueCollection params;
        if (header.has("Content-Disposition"))
        {
            std::string unused;
            Poco::Net::MessageHeader::splitParameters(header.get("Content-Disposition"), unused, params);
        }

        if (params.has("filename"))
            handler.handlePart(header, in);
        else
        {
            std::string name = params["name"];
            std::string value;
            char ch;

            while (in.read(ch))
            {
                if (value.size() > max_field_value_size)
                    throw Poco::Net::HTMLFormException("Field value too long");
                value += ch;
            }

            add(name, value);
        }

        ++fields;

        /// If we already encountered EOF for the buffer |in|, it's possible that the next symbol is a start of boundary line.
        /// In this case reading the boundary line will reset the EOF state, potentially breaking invariant of EOF idempotency -
        /// if there is such invariant in the first place.
        if (!in.skipToNextBoundary())
            break;
    }
}


HTMLForm::MultipartReadBuffer::MultipartReadBuffer(ReadBuffer & in_, const std::string & boundary_)
    : ReadBuffer(nullptr, 0), in(in_), boundary("--" + boundary_)
{
    /// For consistency with |nextImpl()|
    position() = in.position();
}

bool HTMLForm::MultipartReadBuffer::skipToNextBoundary()
{
    if (in.eof())
        return false;

    assert(boundary_hit);

    boundary_hit = false;

    while (!in.eof())
    {
        auto line = readLine(true);
        if (startsWith(line, boundary))
        {
            set(in.position(), 0);
            next();  /// We need to restrict our buffer to size of next available line.
            return !startsWith(line, boundary + "--");
        }
    }

    return false;
}

std::string HTMLForm::MultipartReadBuffer::readLine(bool append_crlf)
{
    std::string line;
    char ch = 0;  // silence "uninitialized" warning from gcc-*

    /// If we don't append CRLF, it means that we may have to prepend CRLF from previous content line, which wasn't the boundary.
    if (in.read(ch))
        line += ch;
    if (in.read(ch))
        line += ch;
    if (append_crlf && line == "\r\n")
        return line;

    while (!in.eof())
    {
        while (in.read(ch) && ch != '\r')
            line += ch;

        if (in.eof()) break;

        assert(ch == '\r');

        if (in.peek(ch) && ch == '\n')
        {
            in.ignore();
            if (append_crlf) line += "\r\n";
            break;
        }

        line += ch;
    }

    return line;
}

bool HTMLForm::MultipartReadBuffer::nextImpl()
{
    if (boundary_hit)
        return false;

    assert(position() >= in.position());

    in.position() = position();

    /// We expect to start from the first symbol after EOL, so we can put checkpoint
    /// and safely try to read til the next EOL and check for boundary.
    in.setCheckpoint();

    /// FIXME: there is an extra copy because we cannot traverse PeekableBuffer from checkpoint to position()
    ///        since it may store different data parts in different sub-buffers,
    ///        anyway calling makeContinuousMemoryFromCheckpointToPos() will also make an extra copy.
    /// According to RFC2046 the preceding CRLF is a part of boundary line.
    std::string line = readLine(false);
    boundary_hit = startsWith(line, "\r\n" + boundary);
    bool has_next = !boundary_hit && !line.empty();

    if (has_next)
        /// If we don't make sure that memory is contiguous then situation may happen, when part of the line is inside internal memory
        /// and other part is inside sub-buffer, thus we'll be unable to setup our working buffer properly.
        in.makeContinuousMemoryFromCheckpointToPos();

    in.rollbackToCheckpoint(true);

    /// Rolling back to checkpoint may change underlying buffers.
    /// Limit readable data to a single line.
    BufferBase::set(in.position(), line.size(), 0);

    return has_next;
}

}
