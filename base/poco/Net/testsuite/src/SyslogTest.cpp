//
// SyslogTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SyslogTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/RemoteSyslogChannel.h"
#include "Poco/Net/RemoteSyslogListener.h"
#include "Poco/Net/DNS.h"
#include "Poco/Thread.h"
#include "Poco/Message.h"
#include "Poco/AutoPtr.h"
#include <list>


using namespace Poco::Net;


class CachingChannel: public Poco::Channel
	/// Caches the last n Messages in memory
{
public:
	typedef std::list<Poco::Message> Messages;
	
	CachingChannel(std::size_t n = 100);
		/// Creates the CachingChannel. Caches n messages in memory

	~CachingChannel();
		/// Destroys the CachingChannel.

	void log(const Poco::Message& msg);
		/// Writes the log message to the cache

	void getMessages(std::vector<Poco::Message>& msg, int offset, int numEntries) const;
		/// Retrieves numEntries Messages starting with position offset. Most recent messages are first.

	std::size_t getMaxSize() const;

	std::size_t getCurrentSize() const;

private:
	CachingChannel(const CachingChannel&);

	Messages   _cache;
	std::size_t _size;
	std::size_t _maxSize;
	mutable Poco::FastMutex _mutex;
};


std::size_t CachingChannel::getMaxSize() const
{
	return _maxSize;
}


std::size_t CachingChannel::getCurrentSize() const
{
	return _size;
}


CachingChannel::CachingChannel(std::size_t n):
	_cache(),
	_size(0),
	_maxSize(n),
	_mutex()
{
}


CachingChannel::~CachingChannel()
{
}


void CachingChannel::log(const Poco::Message& msg)
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	_cache.push_front(msg);
	if (_size == _maxSize)
	{
		_cache.pop_back();
	}
	else
		++_size;
}


void CachingChannel::getMessages(std::vector<Poco::Message>& msg, int offset, int numEntries) const
{
	msg.clear();
	Messages::const_iterator it = _cache.begin();

	while (offset > 0 && it != _cache.end())
		++it;

	while (numEntries > 0 && it != _cache.end())
	{
		msg.push_back(*it);
		++it;
	}
}


SyslogTest::SyslogTest(const std::string& name): CppUnit::TestCase(name)
{
}


SyslogTest::~SyslogTest()
{
}


void SyslogTest::testListener()
{
	Poco::AutoPtr<RemoteSyslogChannel> channel = new RemoteSyslogChannel();
	channel->setProperty("loghost", "127.0.0.1:51400");
	channel->open();
	Poco::AutoPtr<RemoteSyslogListener> listener = new RemoteSyslogListener(51400);
	listener->open();
	CachingChannel cl;
	listener->addChannel(&cl);
	assert (cl.getCurrentSize() == 0);
	Poco::Message msg("asource", "amessage", Poco::Message::PRIO_CRITICAL);
	channel->log(msg);
	Poco::Thread::sleep(1000);
	listener->close();
	channel->close();
	assert (cl.getCurrentSize() == 1);
	std::vector<Poco::Message> msgs;
	cl.getMessages(msgs, 0, 10);
	assert (msgs.size() == 1);
	assert (msgs[0].getSource() == "asource");
	assert (msgs[0].getText() == "amessage");
	assert (msgs[0].getPriority() == Poco::Message::PRIO_CRITICAL);
}


void SyslogTest::testChannelOpenClose()
{
	Poco::AutoPtr<RemoteSyslogChannel> channel = new RemoteSyslogChannel();
	channel->setProperty("loghost", "127.0.0.1:51400");
	channel->open();
	Poco::AutoPtr<RemoteSyslogListener> listener = new RemoteSyslogListener(51400);
	listener->open();
	CachingChannel cl;
	listener->addChannel(&cl);

	assert (cl.getCurrentSize() == 0);
	Poco::Message msg1("source1", "message1", Poco::Message::PRIO_CRITICAL);
	channel->log(msg1);
	Poco::Thread::sleep(1000);
	assert (cl.getCurrentSize() == 1);

	channel->close(); // close and re-open channel
	channel->open();

	Poco::Message msg2("source2", "message2", Poco::Message::PRIO_ERROR);
	channel->log(msg2);
	Poco::Thread::sleep(1000);
	assert (cl.getCurrentSize() == 2);

	listener->close();
	std::vector<Poco::Message> msgs;
	cl.getMessages(msgs, 0, 10);
	assert (msgs.size() == 2);

	assert (msgs[1].getSource() == "source1");
	assert (msgs[1].getText() == "message1");
	assert (msgs[1].getPriority() == Poco::Message::PRIO_CRITICAL);

	assert (msgs[0].getSource() == "source2");
	assert (msgs[0].getText() == "message2");
	assert (msgs[0].getPriority() == Poco::Message::PRIO_ERROR);
}


void SyslogTest::testOldBSD()
{
	Poco::AutoPtr<RemoteSyslogChannel> channel = new RemoteSyslogChannel();
	channel->setProperty("loghost", "127.0.0.1:51400");
	channel->setProperty("format", "bsd");
	channel->open();
	Poco::AutoPtr<RemoteSyslogListener> listener = new RemoteSyslogListener(51400);
	listener->open();
	CachingChannel cl;
	listener->addChannel(&cl);
	assert (cl.getCurrentSize() == 0);
	Poco::Message msg("asource", "amessage", Poco::Message::PRIO_CRITICAL);
	channel->log(msg);
	Poco::Thread::sleep(1000);
	listener->close();
	channel->close();
	assert (cl.getCurrentSize() == 1);
	std::vector<Poco::Message> msgs;
	cl.getMessages(msgs, 0, 10);
	assert (msgs.size() == 1);
	// the source is lost with old BSD messages: we only send the local host name!
	assert (msgs[0].getSource() == Poco::Net::DNS::thisHost().name());
	assert (msgs[0].getText() == "amessage");
	assert (msgs[0].getPriority() == Poco::Message::PRIO_CRITICAL);
}


void SyslogTest::testStructuredData()
{
	Poco::AutoPtr<RemoteSyslogChannel> channel = new RemoteSyslogChannel();
	channel->setProperty("loghost", "127.0.0.1:51400");
	channel->open();
	Poco::AutoPtr<RemoteSyslogListener> listener = new RemoteSyslogListener(51400);
	listener->open();
	CachingChannel cl;
	listener->addChannel(&cl);
	assert(cl.getCurrentSize() == 0);
	Poco::Message msg1("asource", "amessage", Poco::Message::PRIO_CRITICAL);
	msg1.set("structured-data", "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]");
	channel->log(msg1);
	Poco::Message msg2("asource", "amessage", Poco::Message::PRIO_CRITICAL);
	msg2.set("structured-data", "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"]");
	channel->log(msg2);
	Poco::Thread::sleep(1000);
	listener->close();
	channel->close();
	assert(cl.getCurrentSize() == 2);
	std::vector<Poco::Message> msgs;
	cl.getMessages(msgs, 0, 10);
	assert(msgs.size() == 2);

	assert(msgs[0].getSource() == "asource");
	assert(msgs[0].getText() == "amessage");
	assert(msgs[0].getPriority() == Poco::Message::PRIO_CRITICAL);
	assert(msgs[0].get("structured-data") == "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"]");

	assert(msgs[1].getSource() == "asource");
	assert(msgs[1].getText() == "amessage");
	assert(msgs[1].getPriority() == Poco::Message::PRIO_CRITICAL);
	assert(msgs[1].get("structured-data") == "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]");
}


void SyslogTest::setUp()
{
}


void SyslogTest::tearDown()
{
}


CppUnit::Test* SyslogTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SyslogTest");

	CppUnit_addTest(pSuite, SyslogTest, testListener);
	CppUnit_addTest(pSuite, SyslogTest, testChannelOpenClose);
	CppUnit_addTest(pSuite, SyslogTest, testOldBSD);
	CppUnit_addTest(pSuite, SyslogTest, testStructuredData);

	return pSuite;
}
