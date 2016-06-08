//
// ChannelTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ChannelTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ChannelTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SplitterChannel.h"
#include "Poco/AsyncChannel.h"
#include "Poco/AutoPtr.h"
#include "Poco/Message.h"
#include "Poco/Formatter.h"
#include "Poco/FormattingChannel.h"
#include "Poco/ConsoleChannel.h"
#include "Poco/StreamChannel.h"
#include "TestChannel.h"
#include <sstream>


using Poco::SplitterChannel;
using Poco::AsyncChannel;
using Poco::FormattingChannel;
using Poco::ConsoleChannel;
using Poco::StreamChannel;
using Poco::Formatter;
using Poco::Message;
using Poco::AutoPtr;


class SimpleFormatter: public Formatter
{
public:
	void format(const Message& msg, std::string& text)
	{
		text = msg.getSource();
		text.append(": ");
		text.append(msg.getText());
	}
};


ChannelTest::ChannelTest(const std::string& name): CppUnit::TestCase(name)
{
}


ChannelTest::~ChannelTest()
{
}


void ChannelTest::testSplitter()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	AutoPtr<SplitterChannel> pSplitter = new SplitterChannel;
	pSplitter->addChannel(pChannel.get());
	pSplitter->addChannel(pChannel.get());
	Message msg;
	pSplitter->log(msg);
	assert (pChannel->list().size() == 2);
}


void ChannelTest::testAsync()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	AutoPtr<AsyncChannel> pAsync = new AsyncChannel(pChannel.get());
	pAsync->open();
	Message msg;
	pAsync->log(msg);
	pAsync->log(msg);
	pAsync->close();
	assert (pChannel->list().size() == 2);
}


void ChannelTest::testFormatting()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	AutoPtr<Formatter> pFormatter = new SimpleFormatter;
	AutoPtr<FormattingChannel> pFormatterChannel = new FormattingChannel(pFormatter, pChannel.get());
	Message msg("Source", "Text", Message::PRIO_INFORMATION);
	pFormatterChannel->log(msg);
	assert (pChannel->list().size() == 1);
	assert (pChannel->list().begin()->getText() == "Source: Text");
}


void ChannelTest::testConsole()
{
	AutoPtr<ConsoleChannel> pChannel = new ConsoleChannel;
	AutoPtr<Formatter> pFormatter = new SimpleFormatter;
	AutoPtr<FormattingChannel> pFormatterChannel = new FormattingChannel(pFormatter, pChannel.get());
	Message msg("Source", "Text", Message::PRIO_INFORMATION);
	pFormatterChannel->log(msg);
}


void ChannelTest::testStream()
{
	std::ostringstream str;
	AutoPtr<StreamChannel> pChannel = new StreamChannel(str);
	AutoPtr<Formatter> pFormatter = new SimpleFormatter;
	AutoPtr<FormattingChannel> pFormatterChannel = new FormattingChannel(pFormatter, pChannel.get());
	Message msg("Source", "Text", Message::PRIO_INFORMATION);
	pFormatterChannel->log(msg);
	assert (str.str().find("Source: Text") == 0);
}


void ChannelTest::setUp()
{
}


void ChannelTest::tearDown()
{
}


CppUnit::Test* ChannelTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ChannelTest");

	CppUnit_addTest(pSuite, ChannelTest, testSplitter);
	CppUnit_addTest(pSuite, ChannelTest, testAsync);
	CppUnit_addTest(pSuite, ChannelTest, testFormatting);
	CppUnit_addTest(pSuite, ChannelTest, testConsole);
	CppUnit_addTest(pSuite, ChannelTest, testStream);

	return pSuite;
}
