//
// Event.cpp
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


#include "Poco/DOM/Event.h"
#include "Poco/DOM/Document.h"


namespace Poco {
namespace XML {


Event::Event(Document* pOwnerDocument, const XMLString& type):
	_pOwner(pOwnerDocument),
	_type(type),
	_pTarget(0),
	_pCurrentTarget(0),
	_currentPhase(CAPTURING_PHASE),
	_bubbles(true),
	_cancelable(true),
	_canceled(false),
	_stopped(false)
{
}


Event::Event(Document* pOwnerDocument, const XMLString& type, EventTarget* pTarget, bool canBubble, bool isCancelable):
	_pOwner(pOwnerDocument),
	_type(type),
	_pTarget(pTarget),
	_pCurrentTarget(0),
	_currentPhase(CAPTURING_PHASE),
	_bubbles(canBubble),
	_cancelable(isCancelable),
	_canceled(false),
	_stopped(false)
{
}


Event::~Event()
{
}


void Event::stopPropagation()
{
	_stopped = true;
}


void Event::preventDefault()
{
	_canceled = true;
}


void Event::initEvent(const XMLString& eventType, bool canBubble, bool isCancelable)
{
	_type       = eventType;
	_bubbles    = canBubble;
	_cancelable = isCancelable;
	_canceled   = false;
	_stopped    = false;
}


void Event::setTarget(EventTarget* pTarget)
{
	_pTarget = pTarget;
}


void Event::setCurrentPhase(PhaseType phase)
{
	_currentPhase = phase;
}


void Event::setCurrentTarget(EventTarget* pTarget)
{
	_pCurrentTarget = pTarget;
}


void Event::autoRelease()
{
	_pOwner->autoReleasePool().add(this);
}


} } // namespace Poco::XML
