#include <Server/HTTP/HTMLForm.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromString.h>

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

const std::string HTMLForm::ENCODING_URL = "application/x-www-form-urlencoded";
const std::string HTMLForm::ENCODING_MULTIPART = "multipart/form-data";
const int HTMLForm::UNKNOWN_CONTENT_LENGTH = -1;


class HTMLFormCountingOutputStream : public Poco::CountingOutputStream
{
public:
    HTMLFormCountingOutputStream() : valid(true) { }

    bool isValid() const { return valid; }

    void setValid(bool v) { valid = v; }

private:
    bool valid;
};


HTMLForm::HTMLForm() : field_limit(DFL_FIELD_LIMIT), value_length_limit(DFL_MAX_VALUE_LENGTH), encoding(ENCODING_URL)
{
}


HTMLForm::HTMLForm(const std::string & encoding_)
    : field_limit(DFL_FIELD_LIMIT), value_length_limit(DFL_MAX_VALUE_LENGTH), encoding(encoding_)
{
}


HTMLForm::HTMLForm(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, Poco::Net::PartHandler & handler)
    : field_limit(DFL_FIELD_LIMIT), value_length_limit(DFL_MAX_VALUE_LENGTH)
{
    load(request, requestBody, handler);
}


HTMLForm::HTMLForm(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody)
    : field_limit(DFL_FIELD_LIMIT), value_length_limit(DFL_MAX_VALUE_LENGTH)
{
    load(request, requestBody);
}


HTMLForm::HTMLForm(const Poco::Net::HTTPRequest & request) : HTMLForm(Poco::URI(request.getURI()))
{
}

HTMLForm::HTMLForm(const Poco::URI & uri) : field_limit(DFL_FIELD_LIMIT), value_length_limit(DFL_MAX_VALUE_LENGTH)
{
    ReadBufferFromString istr(uri.getRawQuery()); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    readUrl(istr);
}


void HTMLForm::setEncoding(const std::string & encoding_)
{
    encoding = encoding_;
}


void HTMLForm::addPart(const std::string & name, Poco::Net::PartSource * source)
{
    poco_check_ptr(source);

    Part part;
    part.name = name;
    part.source = std::unique_ptr<Poco::Net::PartSource>(source);
    parts.push_back(std::move(part));
}


void HTMLForm::load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, Poco::Net::PartHandler & handler)
{
    clear();

    Poco::URI uri(request.getURI());
    const std::string & query = uri.getRawQuery();
    if (!query.empty())
    {
        ReadBufferFromString istr(query);
        readUrl(istr);
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
            readUrl(requestBody);
        }
    }
}


void HTMLForm::load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody)
{
    Poco::Net::NullPartHandler nah;
    load(request, requestBody, nah);
}


void HTMLForm::load(const Poco::Net::HTTPRequest & request)
{
    Poco::Net::NullPartHandler nah;
    EmptyReadBuffer nis;
    load(request, nis, nah);
}


void HTMLForm::read(ReadBuffer & in, Poco::Net::PartHandler & handler)
{
    if (encoding == ENCODING_URL)
        readUrl(in);
    else
        readMultipart(in, handler);
}


void HTMLForm::read(ReadBuffer & in)
{
    readUrl(in);
}


void HTMLForm::read(const std::string & queryString)
{
    ReadBufferFromString istr(queryString);
    readUrl(istr);
}


void HTMLForm::prepareSubmit(Poco::Net::HTTPRequest & request, int options)
{
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST || request.getMethod() == Poco::Net::HTTPRequest::HTTP_PUT)
    {
        if (encoding == ENCODING_URL)
        {
            request.setContentType(encoding);
            request.setChunkedTransferEncoding(false);
            Poco::CountingOutputStream ostr;
            writeUrl(ostr);
            request.setContentLength(ostr.chars());
        }
        else
        {
            boundary = Poco::Net::MultipartWriter::createBoundary();
            std::string ct(encoding);
            ct.append("; boundary=\"");
            ct.append(boundary);
            ct.append("\"");
            request.setContentType(ct);
        }
        if (request.getVersion() == Poco::Net::HTTPMessage::HTTP_1_0)
        {
            request.setKeepAlive(false);
            request.setChunkedTransferEncoding(false);
        }
        else if (encoding != ENCODING_URL && (options & OPT_USE_CONTENT_LENGTH) == 0)
        {
            request.setChunkedTransferEncoding(true);
        }
        if (!request.getChunkedTransferEncoding() && !request.hasContentLength())
        {
            request.setContentLength(calculateContentLength());
        }
    }
    else
    {
        std::string uri = request.getURI();
        std::ostringstream ostr;
        writeUrl(ostr);
        uri.append("?");
        uri.append(ostr.str());
        request.setURI(uri);
    }
}


std::streamsize HTMLForm::calculateContentLength()
{
    if (encoding == ENCODING_MULTIPART && boundary.empty())
        throw Poco::Net::HTMLFormException("Form must be prepared");

    HTMLFormCountingOutputStream c;
    write(c);
    if (c.isValid())
        return c.chars();
    else
        return UNKNOWN_CONTENT_LENGTH;
}


void HTMLForm::write(std::ostream & ostr, const std::string & boundary_)
{
    if (encoding == ENCODING_URL)
    {
        writeUrl(ostr);
    }
    else
    {
        boundary = boundary_;
        writeMultipart(ostr);
    }
}


void HTMLForm::write(std::ostream & ostr)
{
    if (encoding == ENCODING_URL)
        writeUrl(ostr);
    else
        writeMultipart(ostr);
}


void HTMLForm::readUrl(ReadBuffer & in)
{
    int fields = 0;
    char ch;
    in.read(ch);
    bool is_first = true;
    while (!in.eof())
    {
        if (field_limit > 0 && fields == field_limit)
            throw Poco::Net::HTMLFormException("Too many form fields");
        std::string name;
        std::string value;
        while (!in.eof() && ch != '=' && ch != '&')
        {
            if (ch == '+')
                ch = ' ';
            if (name.size() < MAX_NAME_LENGTH)
                name += ch;
            else
                throw Poco::Net::HTMLFormException("Field name too long");
            in.read(ch);
        }
        if (ch == '=')
        {
            in.read(ch);
            while (!in.eof() && ch != '&')
            {
                if (ch == '+')
                    ch = ' ';
                if (value.size() < value_length_limit)
                    value += char(ch);
                else
                    throw Poco::Net::HTMLFormException("Field value too long");
                in.read(ch);
            }
        }
        // remove UTF-8 byte order mark from first name, if present
        if (is_first)
        {
            Poco::UTF8::removeBOM(name);
        }
        std::string decoded_name;
        std::string decoded_value;
        Poco::URI::decode(name, decoded_name);
        Poco::URI::decode(value, decoded_value);
        add(decoded_name, decoded_value);
        ++fields;
        if (ch == '&')
            in.read(ch);
        is_first = false;
    }
}


void HTMLForm::readMultipart(ReadBuffer & /* in */, Poco::Net::PartHandler & /* handler */)
{
    /// TODO: drop multi-part support for now

    // int fields = 0;
    // Poco::Net::MultipartReader reader(in, boundary);
    // while (reader.hasNextPart())
    // {
    //     if (field_limit > 0 && fields == field_limit)
    //         throw Poco::Net::HTMLFormException("Too many form fields");
    //     Poco::Net::MessageHeader header;
    //     reader.nextPart(header);
    //     std::string disp;
    //     NameValueCollection params;
    //     if (header.has("Content-Disposition"))
    //     {
    //         std::string cd = header.get("Content-Disposition");
    //         Poco::Net::MessageHeader::splitParameters(cd, disp, params);
    //     }
    //     if (params.has("filename"))
    //     {
    //         handler.handlePart(header, reader.stream());
    //         // Ensure that the complete part has been read.
    //         while (reader.stream().good())
    //             reader.stream().get();
    //     }
    //     else
    //     {
    //         std::string name = params["name"];
    //         std::string value;
    //         std::istream & istr = reader.stream();
    //         char ch;
    //         in.read(ch);
    //         while (!in.eof())
    //         {
    //             if (value.size() < value_length_limit)
    //                 value += ch;
    //             else
    //                 throw Poco::Net::HTMLFormException("Field value too long");
    //             ch = istr.get();
    //         }
    //         add(name, value);
    //     }
    //     ++fields;
    // }
}


void HTMLForm::writeUrl(std::ostream & ostr)
{
    for (NameValueCollection::ConstIterator it = begin(); it != end(); ++it)
    {
        if (it != begin())
            ostr << "&";
        std::string name;
        Poco::URI::encode(it->first, "!?#/'\",;:$&()[]*+=@", name);
        std::string value;
        Poco::URI::encode(it->second, "!?#/'\",;:$&()[]*+=@", value);
        ostr << name << "=" << value;
    }
}


void HTMLForm::writeMultipart(std::ostream & ostr)
{
    HTMLFormCountingOutputStream * counting_output_stream(dynamic_cast<HTMLFormCountingOutputStream *>(&ostr));

    Poco::Net::MultipartWriter writer(ostr, boundary);
    for (const auto & it : *this)
    {
        Poco::Net::MessageHeader header;
        std::string disp("form-data; name=\"");
        disp.append(it.first);
        disp.append("\"");
        header.set("Content-Disposition", disp);
        writer.nextPart(header);
        ostr << it.second;
    }
    for (auto & part : parts)
    {
        Poco::Net::MessageHeader header(part.source->headers());
        std::string disp("form-data; name=\"");
        disp.append(part.name);
        disp.append("\"");
        std::string filename = part.source->filename();
        if (!filename.empty())
        {
            disp.append("; filename=\"");
            disp.append(filename);
            disp.append("\"");
        }
        header.set("Content-Disposition", disp);
        header.set("Content-Type", part.source->mediaType());
        writer.nextPart(header);
        if (counting_output_stream)
        {
            // count only, don't move stream position
            std::streamsize partlen = part.source->getContentLength();
            if (partlen != Poco::Net::PartSource::UNKNOWN_CONTENT_LENGTH)
                counting_output_stream->addChars(static_cast<int>(partlen));
            else
                counting_output_stream->setValid(false);
        }
        else
        {
            Poco::StreamCopier::copyStream(part.source->stream(), ostr);
        }
    }
    writer.close();
    boundary = writer.boundary();
}


void HTMLForm::setFieldLimit(int limit)
{
    poco_assert(limit >= 0);

    field_limit = limit;
}


void HTMLForm::setValueLengthLimit(int limit)
{
    poco_assert(limit >= 0);

    value_length_limit = limit;
}

}
