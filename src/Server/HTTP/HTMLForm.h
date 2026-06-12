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

    const size_t max_fields_number, max_field_name_size, max_field_value_size;

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
    MultipartReadBuffer(ReadBuffer & in, const std::string & boundary);

    /// Returns false if last boundary found.
    bool skipToNextBoundary();

    bool isActualEOF() const { return found_last_boundary; }

private:
    PeekableReadBuffer in;
    const std::string boundary;
    bool boundary_hit = true;
    bool found_last_boundary = false;

    std::string readLine(bool append_crlf);

    bool nextImpl() override;
};

}
