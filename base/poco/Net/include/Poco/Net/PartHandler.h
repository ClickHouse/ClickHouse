//
// PartHandler.h
//
// Library: Net
// Package: Messages
// Module:  PartHandler
//
// Definition of the PartHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_PartHandler_INCLUDED
#define Net_PartHandler_INCLUDED


#include "Poco/Net/Net.h"
#include <istream>


namespace Poco {
namespace Net {


class MessageHeader;


class Net_API PartHandler
	/// The base class for all part or attachment handlers.
	///
	/// Part handlers are used for handling email parts and 
	/// attachments in MIME multipart messages, as well as file 
	/// uploads via HTML forms.
	///
	/// Subclasses must override handlePart().
{
public:
	virtual void handlePart(const MessageHeader& header, std::istream& stream) = 0;
		/// Called for every part encountered during the processing
		/// of an email message or an uploaded HTML form.
		///
		/// Information about the part can be extracted from
		/// the given message header. What information can be obtained
		/// from header depends on the kind of part.
		///
		/// The content of the part can be read from stream.
		
protected:
	PartHandler();
		/// Creates the PartHandler.

	virtual ~PartHandler();
		/// Destroys the PartHandler.

private:
	PartHandler(const PartHandler&);
	PartHandler& operator = (const PartHandler&);
};


} } // namespace Poco::Net


#endif // Net_PartHandler_INCLUDED
