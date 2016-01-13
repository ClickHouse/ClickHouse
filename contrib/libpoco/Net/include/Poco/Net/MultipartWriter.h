//
// MultipartWriter.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/MultipartWriter.h#1 $
//
// Library: Net
// Package: Messages
// Module:  MultipartWriter
//
// Definition of the MultipartWriter class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MultipartWriter_INCLUDED
#define Net_MultipartWriter_INCLUDED


#include "Poco/Net/Net.h"
#include <ostream>


namespace Poco {
namespace Net {


class MessageHeader;


class Net_API MultipartWriter
	/// This class is used to write MIME multipart
	/// messages to an output stream.
	///
	/// The format of multipart messages is described
	/// in section 5.1 of RFC 2046.
	///
	/// To create a multipart message, first create
	/// a MultipartWriter object.
	/// Then, for each part, call nextPart() and
	/// write the content to the output stream.
	/// Repeat for all parts. 
	/// After the last part has been written,
	/// call close() to finish the multipart message.
{
public:
	explicit MultipartWriter(std::ostream& ostr);
		/// Creates the MultipartWriter, using the
		/// given output stream.
		///
		/// Creates a random boundary string.

	MultipartWriter(std::ostream& ostr, const std::string& boundary);
		/// Creates the MultipartWriter, using the
		/// given output stream and boundary string.

	~MultipartWriter();
		/// Destroys the MultipartWriter.
		
	void nextPart(const MessageHeader& header);
		/// Opens a new message part and writes
		/// the message boundary string, followed
		/// by the message header to the stream.
		
	void close();
		/// Closes the multipart message and writes
		/// the terminating boundary string.
		///
		/// Does not close the underlying stream.

	std::ostream& stream();
		/// Returns the writer's stream.

	const std::string& boundary() const;
		/// Returns the multipart boundary used by this writer.

	static std::string createBoundary();
		/// Creates a random boundary string.
		///
		/// The string always has the form
		/// MIME_boundary_XXXXXXXXXXXX, where
		/// XXXXXXXXXXXX is a random hexadecimal
		/// number.
		
private:
	MultipartWriter();
	MultipartWriter(const MultipartWriter&);
	MultipartWriter& operator = (const MultipartWriter&);

	std::ostream& _ostr;
	std::string   _boundary;
	bool          _firstPart;
};


//
// inlines
//
inline std::ostream& MultipartWriter::stream()
{
	return _ostr;
}


} } // namespace Poco::Net


#endif // Net_MultipartWriter_INCLUDED
