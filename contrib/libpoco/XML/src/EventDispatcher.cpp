//
// EventDispatcher.cpp
//
// $Id: //poco/1.4/XML/src/EventDispatcher.cpp#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/EventDispatcher.h"
#include "Poco/DOM/Event.h"
#include "Poco/DOM/EventListener.h"


namespace 
{
	class DispatchGuard
	{
	public:
		DispatchGuard(int& count):
			_count(count)
		{
			++_count;
		}
		
		~DispatchGuard()
		{
			--_count;
		}
		
	private:
		int& _count;
	};
}


namespace Poco {
namespace XML {


EventDispatcher::EventDispatcher():
	_inDispatch(0)
{
}

	
EventDispatcher::~EventDispatcher()
{
}


void EventDispatcher::addEventListener(const XMLString& type, EventListener* listener, bool useCapture)
{
	EventListenerItem item;
	item.type       = type;
	item.pListener  = listener;
	item.useCapture = useCapture;
	_listeners.push_front(item);
}


void EventDispatcher::removeEventListener(const XMLString& type, EventListener* listener, bool useCapture)
{
	EventListenerList::iterator it = _listeners.begin();
	while (it != _listeners.end())
	{
		if (it->type == type && it->pListener == listener && it->useCapture == useCapture)
		{
			it->pListener = 0;
		}
		if (!_inDispatch && !it->pListener)
		{
			EventListenerList::iterator del = it++;
			_listeners.erase(del);
		}
		else ++it;
	}
}


void EventDispatcher::dispatchEvent(Event* evt)
{
	DispatchGuard guard(_inDispatch);
	EventListenerList::iterator it = _listeners.begin();
	while (it != _listeners.end())
	{
		if (it->pListener && it->type == evt->type())
		{
			it->pListener->handleEvent(evt);
		}
		if (!it->pListener)
		{
			EventListenerList::iterator del = it++;
			_listeners.erase(del);
		}
		else ++it;
	}
}


void EventDispatcher::captureEvent(Event* evt)
{
	DispatchGuard guard(_inDispatch);
	EventListenerList::iterator it = _listeners.begin();
	while (it != _listeners.end())
	{
		if (it->pListener && it->useCapture && it->type == evt->type())
		{
			it->pListener->handleEvent(evt);
		}
		if (!it->pListener)
		{
			EventListenerList::iterator del = it++;
			_listeners.erase(del);
		}
		else ++it;
	}
}


void EventDispatcher::bubbleEvent(Event* evt)
{
	DispatchGuard guard(_inDispatch);
	EventListenerList::iterator it = _listeners.begin();
	while (it != _listeners.end())
	{
		if (it->pListener && !it->useCapture && it->type == evt->type())
		{
			it->pListener->handleEvent(evt);
		}
		if (!it->pListener)
		{
			EventListenerList::iterator del = it++;
			_listeners.erase(del);
		}
		else ++it;
	}
}


} } // namespace Poco::XML
