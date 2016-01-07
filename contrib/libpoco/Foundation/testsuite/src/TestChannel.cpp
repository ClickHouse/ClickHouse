//
// TestChannel.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TestChannel.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TestChannel.h"


TestChannel::TestChannel()
{
}


TestChannel::~TestChannel()
{
}


void TestChannel::log(const Poco::Message& msg)
{
	_msgList.push_back(msg);
	_lastMessage = msg;
}


TestChannel::MsgList& TestChannel::list()
{
	return _msgList;
}


void TestChannel::clear()
{
	_msgList.clear();
}
