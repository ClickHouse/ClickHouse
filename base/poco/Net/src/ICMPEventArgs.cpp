//
// ICMPEventArgs.cpp
//
// Library: Net
// Package: ICMP
// Module:  ICMPEventArgs
//
// Implementation of ICMPEventArgs
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ICMPEventArgs.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/DNS.h"
#include "Poco/Exception.h"
#include "Poco/Net/NetException.h"
#include <numeric>


using Poco::IOException;
using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


ICMPEventArgs::ICMPEventArgs(const SocketAddress& address, int repetitions, int dataSize, int ttl):
	_address(address), 
	_sent(0),
	_dataSize(dataSize), 
	_ttl(ttl), 
	_rtt(repetitions, 0), 
	_errors(repetitions)
{
}


ICMPEventArgs::~ICMPEventArgs()
{
}


std::string ICMPEventArgs::hostName() const
{
	try
	{
		return DNS::resolve(_address.host().toString()).name();
	}
	catch (HostNotFoundException&) 
	{
	}
	catch (NoAddressFoundException&) 
	{
	}
	catch (DNSException&)
	{
	}
	catch (IOException&)
	{
	}
	return _address.host().toString();
}


std::string ICMPEventArgs::hostAddress() const
{
	return _address.host().toString();
}


void ICMPEventArgs::setRepetitions(int repetitions)
{
	_rtt.clear();
	_rtt.resize(repetitions, 0);
	_errors.assign(repetitions, "");
}


ICMPEventArgs& ICMPEventArgs::operator ++ ()
{
	++_sent;
	return *this;
}


ICMPEventArgs ICMPEventArgs::operator ++ (int)
{
	ICMPEventArgs prev(*this);
	operator ++ ();
	return prev;
}


int ICMPEventArgs::received() const
{
	int received = 0;

	for (int i = 0; i < _rtt.size(); ++i) 
	{
		if (_rtt[i]) ++received;
	}
	return received;
}


void ICMPEventArgs::setError(int index, const std::string& text)
{
	if (index >= _errors.size()) 
		throw InvalidArgumentException("Supplied index exceeds vector capacity.");

	_errors[index] = text;
}


const std::string& ICMPEventArgs::error(int index) const
{
	if (0 == _errors.size()) 
		throw InvalidArgumentException("Supplied index exceeds vector capacity.");

	if (-1 == index) index = _sent - 1;

	return _errors[index];
}


void ICMPEventArgs::setReplyTime(int index, int time)
{
	if (index >= _rtt.size()) 
		throw InvalidArgumentException("Supplied index exceeds array capacity.");
	if (0 == time) time = 1;
	_rtt[index] = time;
}


int ICMPEventArgs::replyTime(int index) const
{
	if (0 == _rtt.size()) 
		throw InvalidArgumentException("Supplied index exceeds array capacity.");

	if (-1 == index) index = _sent - 1;

	return _rtt[index];
}


int ICMPEventArgs::avgRTT() const
{
	if (0 == _rtt.size()) return 0;
	
	return (int) (std::accumulate(_rtt.begin(), _rtt.end(), 0) / _rtt.size());
}


float ICMPEventArgs::percent() const
{
	if (0 == _rtt.size()) return 0;

	return ((float) received() / (float) _rtt.size()) * (float) 100.0;
}


} } // namespace Poco::Net
