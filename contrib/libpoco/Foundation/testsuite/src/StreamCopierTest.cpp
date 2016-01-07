//
// StreamCopierTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/StreamCopierTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "StreamCopierTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/StreamCopier.h"
#include <sstream>


using Poco::StreamCopier;


StreamCopierTest::StreamCopierTest(const std::string& name): CppUnit::TestCase(name)
{
}


StreamCopierTest::~StreamCopierTest()
{
}


void StreamCopierTest::testBufferedCopy()
{
	{
		std::string src;
		for (int i = 0; i < 255; ++i) src += char(i);
		std::istringstream istr(src);
		std::ostringstream ostr;
		std::streamsize n = StreamCopier::copyStream(istr, ostr);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		std::streamsize n = StreamCopier::copyStream(istr, ostr, 100);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		std::streamsize n = StreamCopier::copyStream(istr, ostr, 128);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		std::streamsize n = StreamCopier::copyStream(istr, ostr, 512);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
}


void StreamCopierTest::testUnbufferedCopy()
{
	std::string src;
	for (int i = 0; i < 255; ++i) src += char(i);
	std::istringstream istr(src);
	std::ostringstream ostr;
	std::streamsize n = StreamCopier::copyStreamUnbuffered(istr, ostr);
	assert (ostr.str() == src);
	assert (n == src.size());
}


void StreamCopierTest::testCopyToString()
{
	std::string src;
	for (int i = 0; i < 512; ++i) src += char(i % 256);
	std::istringstream istr(src);
	std::string dest;
	std::streamsize n = StreamCopier::copyToString(istr, dest, 100);
	assert (src == dest);
	assert (n == src.size());
}


#if defined(POCO_HAVE_INT64)
void StreamCopierTest::testBufferedCopy64()
{
	{
		std::string src;
		for (int i = 0; i < 255; ++i) src += char(i);
		std::istringstream istr(src);
		std::ostringstream ostr;
		Poco::UInt64 n = StreamCopier::copyStream64(istr, ostr);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		Poco::UInt64 n = StreamCopier::copyStream64(istr, ostr, 100);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		Poco::UInt64 n = StreamCopier::copyStream64(istr, ostr, 128);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
	{
		std::string src;
		for (int i = 0; i < 512; ++i) src += char(i % 256);
		std::istringstream istr(src);
		std::ostringstream ostr;
		Poco::UInt64 n = StreamCopier::copyStream64(istr, ostr, 512);
		assert (ostr.str() == src);
		assert (n == src.size());
	}
}


void StreamCopierTest::testUnbufferedCopy64()
{
	std::string src;
	for (int i = 0; i < 255; ++i) src += char(i);
	std::istringstream istr(src);
	std::ostringstream ostr;
	Poco::UInt64 n = StreamCopier::copyStreamUnbuffered64(istr, ostr);
	assert (ostr.str() == src);
	assert (n == src.size());
}


void StreamCopierTest::testCopyToString64()
{
	std::string src;
	for (int i = 0; i < 512; ++i) src += char(i % 256);
	std::istringstream istr(src);
	std::string dest;
	Poco::UInt64 n = StreamCopier::copyToString64(istr, dest, 100);
	assert (src == dest);
	assert (n == src.size());
}
#endif


void StreamCopierTest::setUp()
{
}


void StreamCopierTest::tearDown()
{
}


CppUnit::Test* StreamCopierTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("StreamCopierTest");

	CppUnit_addTest(pSuite, StreamCopierTest, testBufferedCopy);
	CppUnit_addTest(pSuite, StreamCopierTest, testUnbufferedCopy);
	CppUnit_addTest(pSuite, StreamCopierTest, testCopyToString);

#if defined(POCO_HAVE_INT64)
	CppUnit_addTest(pSuite, StreamCopierTest, testBufferedCopy64);
	CppUnit_addTest(pSuite, StreamCopierTest, testUnbufferedCopy64);
	CppUnit_addTest(pSuite, StreamCopierTest, testCopyToString64);
#endif

	return pSuite;
}
