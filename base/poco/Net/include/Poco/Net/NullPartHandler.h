//
// NullPartHandler.h
//
// Library: Net
// Package: Messages
// Module:  NullPartHandler
//
// Definition of the NullPartHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_NullPartHandler_INCLUDED
#define Net_NullPartHandler_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/PartHandler.h"


namespace Poco {
namespace Net {


class Net_API NullPartHandler: public PartHandler
	/// A very special PartHandler that simply discards all data.
{
public:
	NullPartHandler();
		/// Creates the NullPartHandler.
	
	~NullPartHandler();
		/// Destroys the NullPartHandler.
	
	void handlePart(const MessageHeader& header, std::istream& stream);
		/// Reads and discards all data from the stream.
};


} } // namespace Poco::Net


#endif // Net_NullPartHandler_INCLUDED
