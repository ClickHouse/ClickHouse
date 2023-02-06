//
// PartSource.h
//
// Library: Net
// Package: Messages
// Module:  PartSource
//
// Definition of the PartSource class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_PartSource_INCLUDED
#define Net_PartSource_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/MessageHeader.h"
#include <istream>


namespace Poco {
namespace Net {


class Net_API PartSource
	/// This abstract class is used for adding parts or attachments
	/// to mail messages, as well as for uploading files as part of a HTML form.
{
public:
	virtual std::istream& stream() = 0;
		/// Returns an input stream for reading the
		/// part data.
		///
		/// Subclasses must override this method.

	virtual const std::string& filename() const;
		/// Returns the filename for the part or attachment.
		///
		/// May be overridded by subclasses. The default
		/// implementation returns an empty string.

	const std::string& mediaType() const;
		/// Returns the MIME media type for this part or attachment.

	MessageHeader& headers();
		/// Returns a MessageHeader containing additional header
		/// fields for the part.

	const MessageHeader& headers() const;
		/// Returns a MessageHeader containing additional header
		/// fields for the part.

	virtual std::streamsize getContentLength() const;
		/// Returns the content length for this part
		/// which may be UNKNOWN_CONTENT_LENGTH if
		/// not available.

	virtual ~PartSource();
		/// Destroys the PartSource.

	static const int UNKNOWN_CONTENT_LENGTH;

protected:
	PartSource();
		/// Creates the PartSource, using
		/// the application/octet-stream MIME type.

	PartSource(const std::string& mediaType);
		/// Creates the PartSource, using the
		/// given MIME type.

private:
	PartSource(const PartSource&);
	PartSource& operator = (const PartSource&);

	std::string _mediaType;
	MessageHeader _headers;
};


//
// inlines
//
inline const std::string& PartSource::mediaType() const
{
	return _mediaType;
}


inline MessageHeader& PartSource::headers()
{
	return _headers;
}


inline const MessageHeader& PartSource::headers() const
{
	return _headers;
}


} } // namespace Poco::Net


#endif // Net_PartSource_INCLUDED
