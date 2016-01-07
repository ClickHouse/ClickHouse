//
// LoggingRegistryTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggingRegistryTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LoggingRegistryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/LoggingRegistry.h"
#include "Poco/ConsoleChannel.h"
#include "Poco/PatternFormatter.h"
#include "Poco/AutoPtr.h"

GCC_DIAG_OFF(unused-variable)

using Poco::LoggingRegistry;
using Poco::Channel;
using Poco::ConsoleChannel;
using Poco::Formatter;
using Poco::PatternFormatter;
using Poco::AutoPtr;


LoggingRegistryTest::LoggingRegistryTest(const std::string& name): CppUnit::TestCase(name)
{
}


LoggingRegistryTest::~LoggingRegistryTest()
{
}


void LoggingRegistryTest::testRegister()
{
	LoggingRegistry& reg = LoggingRegistry::defaultRegistry();
	
	reg.clear();
	
	AutoPtr<Channel> pC1 = new ConsoleChannel();
	AutoPtr<Channel> pC2 = new ConsoleChannel();
	AutoPtr<Formatter> pF1 = new PatternFormatter("");
	AutoPtr<Formatter> pF2 = new PatternFormatter("");
	
	reg.registerChannel("c1", pC1);
	reg.registerChannel("c2", pC2);
	reg.registerFormatter("f1", pF1);
	reg.registerFormatter("f2", pF2);

	Channel* pC = reg.channelForName("c1");
	assert (pC1 == pC);
	pC = reg.channelForName("c2");
	assert (pC2 == pC);
	
	Formatter* pF = reg.formatterForName("f1");
	assert (pF1 == pF);
	pF = reg.formatterForName("f2");
	assert (pF2 == pF);
	
	try
	{
		pC = reg.channelForName("c3");
		fail("not found - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
}


void LoggingRegistryTest::testReregister()
{
	LoggingRegistry& reg = LoggingRegistry::defaultRegistry();
	
	reg.clear();
	
	AutoPtr<Channel> pC1 = new ConsoleChannel();
	AutoPtr<Channel> pC2 = new ConsoleChannel();
	AutoPtr<Channel> pC1b = new ConsoleChannel();
	AutoPtr<Formatter> pF1 = new PatternFormatter("");
	AutoPtr<Formatter> pF2 = new PatternFormatter("");
	AutoPtr<Formatter> pF1b = new PatternFormatter("");
	
	reg.registerChannel("c1", pC1);
	reg.registerChannel("c2", pC2);
	reg.registerFormatter("f1", pF1);
	reg.registerFormatter("f2", pF2);
	
	reg.registerChannel("c1", pC1b);
	Channel* pC = reg.channelForName("c1");
	assert (pC1b == pC);
	pC = reg.channelForName("c2");
	assert (pC2 == pC);

	reg.registerFormatter("f1", pF1b);
	Formatter* pF = reg.formatterForName("f1");
	assert (pF1b == pF);
	pF = reg.formatterForName("f2");
	assert (pF2 == pF);
	
}


void LoggingRegistryTest::testUnregister()
{
	LoggingRegistry& reg = LoggingRegistry::defaultRegistry();
	
	reg.clear();
	
	AutoPtr<Channel> pC1 = new ConsoleChannel();
	AutoPtr<Channel> pC2 = new ConsoleChannel();
	AutoPtr<Formatter> pF1 = new PatternFormatter("");
	AutoPtr<Formatter> pF2 = new PatternFormatter("");
	
	reg.registerChannel("c1", pC1);
	reg.registerChannel("c2", pC2);
	reg.registerFormatter("f1", pF1);
	reg.registerFormatter("f2", pF2);

	reg.unregisterChannel("c1");
	reg.unregisterFormatter("f2");
	
	try
	{
		Channel* pC = reg.channelForName("c1");
		fail("unregistered - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}

	try
	{
		Formatter* pF = reg.formatterForName("f2");
		fail("unregistered - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
}


void LoggingRegistryTest::setUp()
{
}


void LoggingRegistryTest::tearDown()
{
}


CppUnit::Test* LoggingRegistryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LoggingRegistryTest");

	CppUnit_addTest(pSuite, LoggingRegistryTest, testRegister);
	CppUnit_addTest(pSuite, LoggingRegistryTest, testReregister);
	CppUnit_addTest(pSuite, LoggingRegistryTest, testUnregister);

	return pSuite;
}
