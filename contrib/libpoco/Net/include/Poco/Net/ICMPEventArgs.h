//
// ICMPEventArgs.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/ICMPEventArgs.h#1 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPEventArgs
//
// Definition of ICMPEventArgs.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ICMPEventArgs_INCLUDED
#define Net_ICMPEventArgs_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddress.h"
#include <vector>
#include <algorithm>


namespace Poco {
namespace Net {


class Net_API ICMPEventArgs
	/// The purpose of the ICMPEventArgs class is to be used as template parameter
	/// to instantiate event members in ICMPClient class.
	/// When clients register for an event notification, the reference to the class is 
	///	passed to the handler function to provide information about the event.
{
public:
	ICMPEventArgs(const SocketAddress& address, int repetitions, int dataSize, int ttl);
		/// Creates ICMPEventArgs.

	virtual ~ICMPEventArgs();
		/// Destroys ICMPEventArgs.

	std::string hostName() const;
		/// Tries to resolve the target IP address into host name.
		/// If unsuccessful, all exceptions are silently ignored and 
		///	the IP address is returned.

	std::string hostAddress() const;
		/// Returns the target IP address.

	int repetitions() const;
		/// Returns the number of repetitions for the ping operation.

	int dataSize() const;
		/// Returns the packet data size in bytes.

	int ttl() const;
		/// Returns time to live.

	int sent() const;
		/// Returns the number of packets sent.

	int received() const;
		/// Returns the number of packets received.

	int replyTime(int index = -1) const;
		/// Returns the reply time for the request specified with index.
		/// If index == -1 (default), returns the most recent reply time.

	const std::string& error(int index = -1) const;
		/// Returns the error string for the request specified with index.
		/// If index == -1 (default), returns the most recent error string.

	int minRTT() const;
		/// Returns the minimum round trip time for a sequence of requests.

	int maxRTT() const;
		/// Returns the maximum round trip time for a sequence of requests.

	int avgRTT() const;
		/// Returns the average round trip time for a sequence of requests.

	float percent() const;
		/// Returns the success percentage for a sequence of requests.

private:
	ICMPEventArgs();

	void setRepetitions(int repetitions);
	void setDataSize(int sz);
	void setTTL(int timeToLive);
	void setReplyTime(int index, int time);
	void setError(int index, const std::string& text);
	ICMPEventArgs& operator ++ ();
	ICMPEventArgs operator ++ (int);

	SocketAddress _address;
	int _sent;
	int _dataSize;
	int _ttl;
	std::vector<int> _rtt;
	std::vector<std::string> _errors;

	friend class ICMPClient;
};


//
// inlines
//
inline int ICMPEventArgs::repetitions() const
{
	return (int) _rtt.size();
}


inline void ICMPEventArgs::setDataSize(int sz)
{
	_dataSize = sz;
}


inline int ICMPEventArgs::dataSize() const
{
	return _dataSize;
}


inline void ICMPEventArgs::setTTL(int timeToLive)
{
	_ttl = timeToLive;
}


inline int ICMPEventArgs::ttl() const
{
	return _ttl;
}


inline int ICMPEventArgs::sent() const
{
	return _sent;
}


inline int ICMPEventArgs::minRTT() const
{
	return *std::min_element(_rtt.begin(), _rtt.end());
}


inline int ICMPEventArgs::maxRTT() const
{
	return *std::max_element(_rtt.begin(), _rtt.end());
}


} } // namespace Poco::Net


#endif
