//
// HTMLForm.h
//
// Library: Net
// Package: HTML
// Module:  HTMLForm
//
// Definition of the HTMLForm class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTMLForm_INCLUDED
#define Net_HTMLForm_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/NameValueCollection.h"
#include <ostream>
#include <istream>
#include <vector>


namespace Poco {
namespace Net {


class HTTPRequest;
class PartHandler;
class PartSource;


class Net_API HTMLForm: public NameValueCollection
	/// HTMLForm is a helper class for working with HTML forms,
	/// both on the client and on the server side.
	///
	/// The maximum number of form fields can be restricted
	/// by calling setFieldLimit(). This is useful to
	/// defend against certain kinds of denial-of-service
	/// attacks. The limit is only enforced when parsing
	/// form data from a stream or string, not when adding
	/// form fields programmatically. The default limit is 100.
{
public:
	enum Options
	{
		OPT_USE_CONTENT_LENGTH = 0x01
			/// Don't use Chunked Transfer-Encoding for multipart requests.
	};

	HTMLForm();
		/// Creates an empty HTMLForm and sets the
		/// encoding to "application/x-www-form-urlencoded".

	explicit HTMLForm(const std::string& encoding);
		/// Creates an empty HTMLForm that uses
		/// the given encoding.
		///
		/// Encoding must be either "application/x-www-form-urlencoded"
		/// (which is the default) or "multipart/form-data".

	HTMLForm(const HTTPRequest& request, std::istream& requestBody, PartHandler& handler);
		/// Creates a HTMLForm from the given HTTP request.
		///
		/// Uploaded files are passed to the given PartHandler.

	HTMLForm(const HTTPRequest& request, std::istream& requestBody);
		/// Creates a HTMLForm from the given HTTP request.
		///
		/// Uploaded files are silently discarded.

	explicit HTMLForm(const HTTPRequest& request);
		/// Creates a HTMLForm from the given HTTP request.
		///
		/// The request must be a GET request and the form data
		/// must be in the query string (URL encoded).
		///
		/// For POST requests, you must use one of the constructors
		/// taking an additional input stream for the request body.

	~HTMLForm();
		/// Destroys the HTMLForm.

	void setEncoding(const std::string& encoding);
		/// Sets the encoding used for posting the form.
		///
		/// Encoding must be either "application/x-www-form-urlencoded"
		/// (which is the default) or "multipart/form-data".

	const std::string& getEncoding() const;
		/// Returns the encoding used for posting the form.

	void addPart(const std::string& name, PartSource* pSource);
		/// Adds an part/attachment (file upload) to the form.
		///
		/// The form takes ownership of the PartSource and deletes it
		/// when it is no longer needed.
		///
		/// The part will only be sent if the encoding
		/// set for the form is "multipart/form-data"

	void load(const HTTPRequest& request, std::istream& requestBody, PartHandler& handler);
		/// Reads the form data from the given HTTP request.
		///
		/// Uploaded files are passed to the given PartHandler.

	void load(const HTTPRequest& request, std::istream& requestBody);
		/// Reads the form data from the given HTTP request.
		///
		/// Uploaded files are silently discarded.

	void load(const HTTPRequest& request);
		/// Reads the form data from the given HTTP request.
		///
		/// The request must be a GET request and the form data
		/// must be in the query string (URL encoded).
		///
		/// For POST requests, you must use one of the overloads
		/// taking an additional input stream for the request body.

	void read(std::istream& istr, PartHandler& handler);
		/// Reads the form data from the given input stream.
		///
		/// The form data read from the stream must be
		/// in the encoding specified for the form.
		///
		/// Note that read() does not clear the form before
		/// reading the new values.

	void read(std::istream& istr);
		/// Reads the URL-encoded form data from the given input stream.
		///
		/// Note that read() does not clear the form before
		/// reading the new values.

	void read(const std::string& queryString);
		/// Reads the form data from the given HTTP query string.
		///
		/// Note that read() does not clear the form before
		/// reading the new values.

	void prepareSubmit(HTTPRequest& request, int options = 0);
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
		/// Note: Not using chunked transfer encoding for multipart forms
		/// degrades performance, as the request content must be generated
		/// twice, first to determine its size, then to actually send it.

	std::streamsize calculateContentLength();
		/// Calculate the content length for the form.
		/// May be UNKNOWN_CONTENT_LENGTH if not possible
		/// to calculate

	void write(std::ostream& ostr, const std::string& boundary);
		/// Writes the form data to the given output stream,
		/// using the specified encoding.

	void write(std::ostream& ostr);
		/// Writes the form data to the given output stream,
		/// using the specified encoding.

	const std::string& boundary() const;
		/// Returns the MIME boundary used for writing
		/// multipart form data.

	int getFieldLimit() const;
		/// Returns the maximum number of header fields
		/// allowed.
		///
		/// See setFieldLimit() for more information.

	void setFieldLimit(int limit);
		/// Sets the maximum number of header fields
		/// allowed. This limit is used to defend certain
		/// kinds of denial-of-service attacks.
		/// Specify 0 for unlimited (not recommended).
		///
		/// The default limit is 100.

	void setValueLengthLimit(int limit);
		/// Sets the maximum size for form field values
		/// stored as strings.

	int getValueLengthLimit() const;
		/// Returns the maximum size for form field values
		/// stored as strings.

	static const std::string ENCODING_URL;       /// "application/x-www-form-urlencoded"
	static const std::string ENCODING_MULTIPART; /// "multipart/form-data"
	static const int         UNKNOWN_CONTENT_LENGTH;

protected:
	void readUrl(std::istream& istr);
	void readMultipart(std::istream& istr, PartHandler& handler);
	void writeUrl(std::ostream& ostr);
	void writeMultipart(std::ostream& ostr);

private:
	HTMLForm(const HTMLForm&);
	HTMLForm& operator = (const HTMLForm&);

	enum Limits
	{
		DFL_FIELD_LIMIT = 100,
		MAX_NAME_LENGTH  = 1024,
		DFL_MAX_VALUE_LENGTH = 256*1024
	};

	struct Part
	{
		std::string name;
		PartSource* pSource;
	};

	typedef std::vector<Part> PartVec;

	int         _fieldLimit;
	int         _valueLengthLimit;
	std::string _encoding;
	std::string _boundary;
	PartVec     _parts;
};


//
// inlines
//
inline const std::string& HTMLForm::getEncoding() const
{
	return _encoding;
}


inline const std::string& HTMLForm::boundary() const
{
	return _boundary;
}


inline int HTMLForm::getFieldLimit() const
{
	return _fieldLimit;
}


inline int HTMLForm::getValueLengthLimit() const
{
	return _valueLengthLimit;
}


} } // namespace Poco::Net


#endif // Net_HTMLForm_INCLUDED
