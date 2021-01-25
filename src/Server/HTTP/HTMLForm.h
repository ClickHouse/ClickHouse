#pragma once

#include <IO/ReadHelpers.h>

#include <boost/noncopyable.hpp>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/NameValueCollection.h>
#include <Poco/Net/PartHandler.h>
#include <Poco/Net/PartSource.h>
#include <Poco/URI.h>

namespace DB
{

class ReadBuffer;

class HTMLForm : public Poco::Net::NameValueCollection, private boost::noncopyable
{
public:
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
    HTMLForm(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, Poco::Net::PartHandler & handler);

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
    void load(const Poco::Net::HTTPRequest & request, ReadBuffer & requestBody, Poco::Net::PartHandler & handler);

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
    void read(ReadBuffer & in, Poco::Net::PartHandler & handler);

	/// Reads the URL-encoded form data from the given input stream.
    /// Note that read() does not clear the form before reading the new values.
    void read(ReadBuffer & in);

	/// Reads the form data from the given HTTP query string.
    /// Note that read() does not clear the form before reading the new values.
    void read(const std::string & queryString);

	/// Fills out the request object for submitting the form.
    ///
    /// If the request method is GET, the encoded form is appended to the
    /// request URI as query string. Otherwise (the method is
    /// POST), the form's content type is set to the form's encoding.
    /// The form's parameters must be written to the
    /// request body separately, with a call to write.
    /// If the request's HTTP version is HTTP/1.0:
    ///    - persistent connections are disabled
    ///    - the content transfer encoding is set to identity encoding
    /// Otherwise, if the request's HTTP version is HTTP/1.1:
    ///    - the request's persistent connection state is left unchanged
    ///    - the content transfer encoding is set to chunked, unless
    ///      the OPT_USE_CONTENT_LENGTH is given in options
    ///
    /// NOTE: Not using chunked transfer encoding for multipart forms
    ///       degrades performance, as the request content must be generated
    ///       twice, first to determine its size, then to actually send it.
    void prepareSubmit(Poco::Net::HTTPRequest & request, int options = 0);

	/// Calculate the content length for the form.
    /// May be UNKNOWN_CONTENT_LENGTH if not possible to calculate.
    std::streamsize calculateContentLength();

	/// Writes the form data to the given output stream, using the specified encoding.
    void write(std::ostream & ostr, const std::string & boundary);

	/// Writes the form data to the given output stream, using the specified encoding.
    void write(std::ostream & ostr);

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
    void readUrl(ReadBuffer & in);
    void readMultipart(ReadBuffer & in, Poco::Net::PartHandler & handler);
    void writeUrl(std::ostream & ostr);
    void writeMultipart(std::ostream & ostr);

private:
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

    int field_limit;
    size_t value_length_limit;
    std::string encoding;
    std::string boundary;
    PartVec parts;
};

}
