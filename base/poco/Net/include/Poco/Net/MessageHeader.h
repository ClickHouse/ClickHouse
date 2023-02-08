//
// MessageHeader.h
//
// Library: Net
// Package: Messages
// Module:  MessageHeader
//
// Definition of the MessageHeader class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MessageHeader_INCLUDED
#define Net_MessageHeader_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/NameValueCollection.h"
#include <ostream>
#include <istream>
#include <vector>


namespace Poco {
namespace Net {


class Net_API MessageHeader: public NameValueCollection
	/// A collection of name-value pairs that are used in
	/// various internet protocols like HTTP and SMTP.
	///
	/// The name is case-insensitive.
	///
	/// There can be more than one name-value pair with the 
	/// same name.
	///
	/// MessageHeader supports writing and reading the
	/// header data in RFC 2822 format.
	///
	/// The maximum number of fields can be restricted
	/// by calling setFieldLimit(). This is useful to
	/// defend against certain kinds of denial-of-service
	/// attacks. The limit is only enforced when parsing
	/// header fields from a stream, not when programmatically
	/// adding them. The default limit is 100.
{
public:
	MessageHeader();
		/// Creates the MessageHeader.

	MessageHeader(const MessageHeader& messageHeader);
		/// Creates the MessageHeader by copying
		/// another one.

	virtual ~MessageHeader();
		/// Destroys the MessageHeader.

	MessageHeader& operator = (const MessageHeader& messageHeader);
		/// Assigns the content of another MessageHeader.

	virtual void write(std::ostream& ostr) const;
		/// Writes the message header to the given output stream.
		///
		/// The format is one name-value pair per line, with
		/// name and value separated by a colon and lines
		/// delimited by a carriage return and a linefeed 
		/// character. See RFC 2822 for details.
		
	virtual void read(std::istream& istr);
		/// Reads the message header from the given input stream.
		///
		/// See write() for the expected format.
		/// Also supported is folding of field content, according
		/// to section 2.2.3 of RFC 2822.
		///
		/// Reading stops at the first empty line (a line only
		/// containing \r\n or \n), as well as at the end of
		/// the stream.
		///
		/// Some basic sanity checking of the input stream is
		/// performed.
		///
		/// Throws a MessageException if the input stream is
		/// malformed.
		
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
	
	bool hasToken(const std::string& fieldName, const std::string& token) const;
		/// Returns true iff the field with the given fieldName contains
		/// the given token. Tokens in a header field are expected to be
		/// comma-separated and are case insensitive.

	static void splitElements(const std::string& s, std::vector<std::string>& elements, bool ignoreEmpty = true);
		/// Splits the given string into separate elements. Elements are expected
		/// to be separated by commas.
		///
		/// For example, the string 
		///   text/plain; q=0.5, text/html, text/x-dvi; q=0.8
		/// is split into the elements
		///   text/plain; q=0.5
		///   text/html
		///   text/x-dvi; q=0.8
		///
		/// Commas enclosed in double quotes do not split elements.
		///
		/// If ignoreEmpty is true, empty elements are not returned.
		
	static void splitParameters(const std::string& s, std::string& value, NameValueCollection& parameters);
		/// Splits the given string into a value and a collection of parameters.
		/// Parameters are expected to be separated by semicolons.
		///
		/// Enclosing quotes of parameter values are removed.
		///
		/// For example, the string
		///   multipart/mixed; boundary="MIME_boundary_01234567"
		/// is split into the value
		///   multipart/mixed
		/// and the parameter
		///   boundary -> MIME_boundary_01234567

	static void splitParameters(const std::string::const_iterator& begin, const std::string::const_iterator& end, NameValueCollection& parameters);
		/// Splits the given string into a collection of parameters.
		/// Parameters are expected to be separated by semicolons.
		///
		/// Enclosing quotes of parameter values are removed.

	static void quote(const std::string& value, std::string& result, bool allowSpace = false);
		/// Checks if the value must be quoted. If so, the value is
		/// appended to result, enclosed in double-quotes.
		/// Otherwise, the value is appended to result as-is.
		
	static void decodeRFC2047(const std::string& ins, std::string& outs, const std::string& charset = "UTF-8");
	static std::string decodeWord(const std::string& text, const std::string& charset = "UTF-8");
	        /// Decode RFC2047 string.

		
private:
	enum Limits
		/// Limits for basic sanity checks when reading a header
	{
		MAX_NAME_LENGTH  = 256,
		MAX_VALUE_LENGTH = 8192,
		DFL_FIELD_LIMIT  = 100
	};
	
	int _fieldLimit;
};


} } // namespace Poco::Net


#endif // Net_MessageHeader_INCLUDED
