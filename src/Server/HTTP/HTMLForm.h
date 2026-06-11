#pragma once

#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <boost/noncopyable.hpp>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/NameValueCollection.h>
#include <Poco/Net/PartSource.h>
#include <Poco/URI.h>

namespace DB
{

struct Settings;

class HTMLForm : public Poco::Net::NameValueCollection, private boost::noncopyable
{
public:
    class PartHandler;

    enum Options
    {
        OPT_USE_CONTENT_LENGTH = 0x01,  /// don't use Chunked Transfer-Encoding for multipart requests.
    };

    /// Creates an empty HTMLForm and sets the
    /// encoding to "application/x-www-form-urlencoded".
    explicit HTMLForm(const Settings & settings);

    /// Creates an empty HTMLForm that uses the given encoding.
    /// Encoding must be either "application/x-www-form-urlencoded" (which is the default) or "multipart/form-data".
    explicit HTMLForm(const Settings & settings, const std::string & encoding);

    /// Creates a HTMLForm from the given HTTP request.
    /// Uploaded files are passed to the given PartHandler.
    HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler);

    /// Creates a HTMLForm from the given HTTP request.
    /// Uploaded files are silently discarded.
    HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody);

    /// Creates a HTMLForm from the given HTTP request.
    /// The request must be a GET request and the form data must be in the query string (URL encoded).
    /// For POST requests, you must use one of the constructors taking an additional input stream for the request body.
    explicit HTMLForm(const Settings & settings, const Poco::Net::HTTPRequest & request);

    explicit HTMLForm(const Settings & settings, const Poco::URI & uri);

    template <typename T>
    T getParsed(const std::string & key, T default_value)
    {
        auto it = find(key);
        return (it != end()) ? DB::parse<T>(it->second) : default_value;
    }

    template <typename T>
    T getParsedLast(const std::string & key, T default_value)
    {
        auto it = findLast(key);
        return (it != end()) ? DB::parse<T>(it->second) : default_value;
    }

    /// Reads the form data from the given HTTP request.
    /// Uploaded files are passed to the given PartHandler.
    void load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler);

    /// Reads the form data from the given HTTP request.
    /// Uploaded files are silently discarded.
    void load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody);

    /// Reads the URL-encoded form data from the given input stream.
    /// Note that read() does not clear the form before reading the new values.
    void read(ReadBuffer & in);

    /// Set the limit on the size of a single line buffered by the multipart parser.
    /// The parser reads the content line by line to detect boundary lines, so part content with
    /// no CRLF would otherwise be accumulated in memory in full, regardless of any limit imposed
    /// on the part size by the reader of the part (in particular, while such a reader only probes
    /// whether more data follows after its limit was reached). A line longer than the whole
    /// multipart/form-data size limit cannot belong to a valid request, so the value of the
    /// `http_max_multipart_form_data_size` setting is used here as well. The constructor
    /// initializes the limit from the settings it is given (typically the server defaults);
    /// this method allows to override it once the authenticated user's settings are known.
    /// Zero disables the limit.
    void setMaxMultipartFormDataSize(size_t limit);

    static const std::string ENCODING_URL; /// "application/x-www-form-urlencoded"
    static const std::string ENCODING_MULTIPART; /// "multipart/form-data"
    static const int UNKNOWN_CONTENT_LENGTH;

protected:
    void readQuery(ReadBuffer & in);
    void readMultipart(ReadBuffer & in, PartHandler & handler);

private:
    /// This buffer provides data line by line to check for boundary line in a convenient way.
    class MultipartReadBuffer;

    struct Part
    {
        std::string name;
        std::unique_ptr<Poco::Net::PartSource> source;
    };

    using PartVec = std::vector<Part>;

    const size_t max_fields_number, max_field_name_size, max_field_value_size, max_request_header_size;

    /// See setMaxMultipartFormDataSize. Stored with a small slack on top of the configured
    /// limit, because a line buffered by the multipart parser may also contain the CRLF
    /// terminating the previous content line of the same part.
    size_t max_multipart_line_size = 0;

    std::string encoding;
    std::string boundary;
    PartVec parts;
};

class HTMLForm::PartHandler
{
public:
    virtual ~PartHandler() = default;
    virtual void handlePart(const Poco::Net::MessageHeader &, ReadBuffer &) = 0;
};

class HTMLForm::MultipartReadBuffer : public ReadBuffer
{
public:
    MultipartReadBuffer(ReadBuffer & in, const std::string & boundary, size_t max_line_size);

    /// Returns false if last boundary found.
    bool skipToNextBoundary();

    bool isActualEOF() const { return found_last_boundary; }

private:
    PeekableReadBuffer in;
    const std::string boundary;
    /// Maximum size of a single buffered line, 0 means no limit (see HTMLForm::setMaxMultipartFormDataSize).
    const size_t max_line_size;
    bool boundary_hit = true;
    bool found_last_boundary = false;

    std::string readLine(bool append_crlf);

    bool nextImpl() override;
};

}
