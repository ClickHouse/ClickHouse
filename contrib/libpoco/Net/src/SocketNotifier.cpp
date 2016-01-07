//
// SocketNotifier.cpp
//
// $Id: //poco/1.4/Net/src/SocketNotifier.cpp#1 $
//
// Library: Net
// Package: Reactor
// Module:  SocketNotifier
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketNotifier.h"
#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/SocketNotification.h"


namespace Poco {
namespace Net {


SocketNotifier::SocketNotifier(const Socket& socket):
	_socket(socket)
{
}

	
SocketNotifier::~SocketNotifier()
{
}

	
void SocketNotifier::addObserver(SocketReactor* pReactor, const Poco::AbstractObserver& observer)
{
	_nc.addObserver(observer);
	if (observer.accepts(pReactor->_pReadableNotification))
		_events.insert(pReactor->_pReadableNotification.get());
	else if (observer.accepts(pReactor->_pWritableNotification))
		_events.insert(pReactor->_pWritableNotification.get());
	else if (observer.accepts(pReactor->_pErrorNotification))
		_events.insert(pReactor->_pErrorNotification.get());
	else if (observer.accepts(pReactor->_pTimeoutNotification))
		_events.insert(pReactor->_pTimeoutNotification.get());
}

	
void SocketNotifier::removeObserver(SocketReactor* pReactor, const Poco::AbstractObserver& observer)
{
	_nc.removeObserver(observer);
	EventSet::iterator it = _events.end();
	if (observer.accepts(pReactor->_pReadableNotification))
		it = _events.find(pReactor->_pReadableNotification.get());
	else if (observer.accepts(pReactor->_pWritableNotification))
		it = _events.find(pReactor->_pWritableNotification.get());
	else if (observer.accepts(pReactor->_pErrorNotification))
		it = _events.find(pReactor->_pErrorNotification.get());
	else if (observer.accepts(pReactor->_pTimeoutNotification))
		it = _events.find(pReactor->_pTimeoutNotification.get());
	if (it != _events.end())
		_events.erase(it);
}


namespace
{
	static Socket nullSocket;
}


void SocketNotifier::dispatch(SocketNotification* pNotification)
{
	pNotification->setSocket(_socket);
	pNotification->duplicate();
	try
	{
		_nc.postNotification(pNotification);
	}
	catch (...)
	{
		pNotification->setSocket(nullSocket);
		throw;
	}
	pNotification->setSocket(nullSocket);
}


} } // namespace Poco::Net
