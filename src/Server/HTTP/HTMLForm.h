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

class HTMLForm : public Poco::Net::NameValueCollection, private boost::noncopyable
{
public:
    class PartHandler;

    enum Options
    {
        OPT_USE_CONTENT_LENGTH = 0x01 // don't use Chunked Transfer-Encoding for multipart requests.
    };

    /// Creates an empty HTMLForm and sets the
    /// encoding to "application/x-www-form-urlencoded".
    HTMLForm();

    /// Creates an empty HTMLForm that uses the given encoding.
    /// Encoding must be either "application/x-www-form-urlencoded" (which is the default) or "multipart/form-data".
    explicit HTMLForm(const std::string & encoding);

    /// Creates a HTMLForm from the given HTTP request.
    /// Uploaded files are passed to the given PartHandler.
    HTMLForm(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler);

    /// Creates a HTMLForm from the given HTTP request.
    /// Uploaded files are silently discarded.
    HTMLForm(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody);

    /// Creates a HTMLForm from the given HTTP request.
    /// The request must be a GET request and the form data must be in the query string (URL encoded).
    /// For POST requests, you must use one of the constructors taking an additional input stream for the request body.
    explicit HTMLForm(const Poco::Net::HTTPRequest & request);

    explicit HTMLForm(const Poco::URI & uri);

    template <typename T>
    T getParsed(const std::string & key, T default_value)
    {
        auto it = find(key);
        return (it != end()) ? DB::parse<T>(it->second) : default_value;
    }

    template <typename T>
    T getParsed(const std::string & key)
    {
        return DB::parse<T>(get(key));
    }

    /// Sets the encoding used for posting the form.
    /// Encoding must be either "application/x-www-form-urlencoded" (which is the default) or "multipart/form-data".
    void setEncoding(const std::string & encoding);

    /// Returns the encoding used for posting the form.
    const std::string & getEncoding() const { return encoding; }

    /// Adds an part/attachment (file upload) to the form.
    /// The form takes ownership of the PartSource and deletes it when it is no longer needed.
    /// The part will only be sent if the encoding set for the form is "multipart/form-data"
    void addPart(const std::string & name, Poco::Net::PartSource * pSource);

    /// Reads the form data from the given HTTP request.
    /// Uploaded files are passed to the given PartHandler.
    void load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, PartHandler & handler);

    /// Reads the form data from the given HTTP request.
    /// Uploaded files are silently discarded.
    void load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody);

    /// Reads the form data from the given HTTP request.
    /// The request must be a GET request and the form data must be in the query string (URL encoded).
    /// For POST requests, you must use one of the overloads taking an additional input stream for the request body.
    void load(const Poco::Net::HTTPRequest & request);

    /// Reads the form data from the given input stream.
    /// The form data read from the stream must be in the encoding specified for the form.
    /// Note that read() does not clear the form before reading the new values.
    void read(ReadBuffer & in, PartHandler & handler);

    /// Reads the URL-encoded form data from the given input stream.
    /// Note that read() does not clear the form before reading the new values.
    void read(ReadBuffer & in);

    /// Reads the form data from the given HTTP query string.
    /// Note that read() does not clear the form before reading the new values.
    void read(const std::string & queryString);

    /// Returns the MIME boundary used for writing multipart form data.
    const std::string & getBoundary() const { return boundary; }

    /// Returns the maximum number of header fields allowed.
    /// See setFieldLimit() for more information.
    int getFieldLimit() const { return field_limit; }

    /// Sets the maximum number of header fields allowed. This limit is used to defend certain kinds of denial-of-service attacks.
    /// Specify 0 for unlimited (not recommended). The default limit is 100.
    void setFieldLimit(int limit);

    /// Sets the maximum size for form field values stored as strings.
    void setValueLengthLimit(int limit);

    /// Returns the maximum size for form field values stored as strings.
    int getValueLengthLimit() const { return value_length_limit; }

    static const std::string ENCODING_URL; /// "application/x-www-form-urlencoded"
    static const std::string ENCODING_MULTIPART; /// "multipart/form-data"
    static const int UNKNOWN_CONTENT_LENGTH;

protected:
    void readQuery(ReadBuffer & in);
    void readMultipart(ReadBuffer & in, PartHandler & handler);

private:
    /// This buffer provides data line by line to check for boundary line in a convenient way.
    class MultipartReadBuffer;

    enum Limits
    {
        DFL_FIELD_LIMIT = 100,
        MAX_NAME_LENGTH = 1024,
        DFL_MAX_VALUE_LENGTH = 256 * 1024
    };

    struct Part
    {
        std::string name;
        std::unique_ptr<Poco::Net::PartSource> source;
    };

    using PartVec = std::vector<Part>;

    size_t field_limit;
    size_t value_length_limit;
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

private:
    PeekableReadBuffer in;
    const std::string boundary;
    bool boundary_hit = true;

    std::string readLine(bool append_crlf);

    bool nextImpl() override;
};

}
