//
// NullChannel.h
//
// Library: Foundation
// Package: Logging
// Module:  NullChannel
//
// Definition of the NullChannel class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NullChannel_INCLUDED
#define Foundation_NullChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"


namespace Poco {


class Foundation_API NullChannel: public Channel
	/// The NullChannel is the /dev/null of Channels.
	///
	/// A NullChannel discards all information sent to it.
	/// Furthermore, its setProperty() method ignores 
	/// all properties, so it the NullChannel has the
	/// nice feature that it can stand in for any
	/// other channel class in a logging configuration.
{
public:
	NullChannel();
		/// Creates the NullChannel.

	~NullChannel();
		/// Destroys the NullChannel.

	void log(const Message& msg);
		/// Does nothing.

	void setProperty(const std::string& name, const std::string& value);
		/// Ignores both name and value.
};


} // namespace Poco


#endif // Foundation_NullChannel_INCLUDED
