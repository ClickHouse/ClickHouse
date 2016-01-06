//
// SplitterChannel.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SplitterChannel.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  SplitterChannel
//
// Definition of the SplitterChannel class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SplitterChannel_INCLUDED
#define Foundation_SplitterChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include <vector>


namespace Poco {


class Foundation_API SplitterChannel: public Channel
	/// This channel sends a message to multiple
	/// channels simultaneously.
{
public:
	SplitterChannel();
		/// Creates the SplitterChannel.

	void addChannel(Channel* pChannel);
		/// Attaches a channel, which may not be null.
		
	void removeChannel(Channel* pChannel);
		/// Removes a channel.

	void log(const Message& msg);
		/// Sends the given Message to all
		/// attaches channels. 

	void setProperty(const std::string& name, const std::string& value);
		/// Sets or changes a configuration property.
		///
		/// Only the "channel" property is supported, which allows
		/// adding a comma-separated list of channels via the LoggingRegistry.
		/// The "channel" property is set-only.
		/// To simplify file-based configuration, all property
		/// names starting with "channel" are treated as "channel".

	void close();
		/// Removes all channels.
		
	int count() const;
		/// Returns the number of channels in the SplitterChannel.

protected:
	~SplitterChannel();

private:
	typedef std::vector<Channel*> ChannelVec;
	
	ChannelVec        _channels;
	mutable FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_SplitterChannel_INCLUDED
