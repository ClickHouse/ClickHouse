//
// SimpleFileChannelTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/SimpleFileChannelTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SimpleFileChannel.h"
#include "Poco/Message.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/AutoPtr.h"
#include "SimpleFileChannelTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"


using Poco::SimpleFileChannel;
using Poco::Message;
using Poco::Path;
using Poco::File;
using Poco::DirectoryIterator;
using Poco::Timestamp;
using Poco::DateTimeFormatter;
using Poco::AutoPtr;


SimpleFileChannelTest::SimpleFileChannelTest(const std::string& name): CppUnit::TestCase(name)
{
}


SimpleFileChannelTest::~SimpleFileChannelTest()
{
}


void SimpleFileChannelTest::testRotate()
{
	std::string name = filename();
	try
	{
		AutoPtr<SimpleFileChannel> pChannel = new SimpleFileChannel(name);
		pChannel->setProperty(SimpleFileChannel::PROP_ROTATION, "2 K");
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		File f(name);
		assert (f.exists());
		f = name + ".0";
		assert (f.exists());
		assert (f.getSize() >= 2048);
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void SimpleFileChannelTest::setUp()
{
}


void SimpleFileChannelTest::tearDown()
{
}


void SimpleFileChannelTest::remove(const std::string& baseName)
{
	DirectoryIterator it(Path::current());
	DirectoryIterator end;
	std::vector<std::string> files;
	while (it != end)
	{
		if (it.name().find(baseName) == 0)
		{
			files.push_back(it.name());
		}
		++it;
	}
	for (std::vector<std::string>::iterator it = files.begin(); it != files.end(); ++it)
	{
		try
		{
			File f(*it);
			f.remove();
		}
		catch (...)
		{
		}
	}
}


std::string SimpleFileChannelTest::filename() const
{
	std::string name = "log_";
	name.append(DateTimeFormatter::format(Timestamp(), "%Y%m%d%H%M%S"));
	name.append(".log");
	return name;
}


CppUnit::Test* SimpleFileChannelTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SimpleFileChannelTest");

	CppUnit_addTest(pSuite, SimpleFileChannelTest, testRotate);

	return pSuite;
}
