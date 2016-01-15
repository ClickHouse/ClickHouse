//
// TestChannel.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TestChannel.h#1 $
//
// Definition of the TestChannel class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TestChannel_INCLUDED
#define TestChannel_INCLUDED


#include "Poco/Channel.h"
#include "Poco/Message.h"
#include <list>


class TestChannel: public Poco::Channel
{
public:
	typedef std::list<Poco::Message> MsgList;

	TestChannel();
	~TestChannel();
	
	void log(const Poco::Message& msg);
	MsgList& list();
	void clear();
	const Poco::Message& getLastMessage() const { return _lastMessage; }
	
private:	
	MsgList _msgList;
	Poco::Message _lastMessage;
};


#endif // TestChannel_INCLUDED
