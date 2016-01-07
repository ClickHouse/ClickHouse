//
// MultipartWriterTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MultipartWriterTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MultipartWriterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MultipartWriter.h"
#include "Poco/Net/MessageHeader.h"
#include <sstream>


using Poco::Net::MultipartWriter;
using Poco::Net::MessageHeader;


MultipartWriterTest::MultipartWriterTest(const std::string& name): CppUnit::TestCase(name)
{
}


MultipartWriterTest::~MultipartWriterTest()
{
}


void MultipartWriterTest::testWriteOnePart()
{
	std::ostringstream ostr;
	MultipartWriter w(ostr, "MIME_boundary_01234567");
	assert (w.boundary() == "MIME_boundary_01234567");
	MessageHeader h;
	h.set("name1", "value1");
	w.nextPart(h);
	ostr << "this is part 1";
	w.close();
	std::string s = ostr.str();	
	assert (s == "--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567--\r\n");
}


void MultipartWriterTest::testWriteTwoParts()
{
	std::ostringstream ostr;
	MultipartWriter w(ostr, "MIME_boundary_01234567");
	MessageHeader h;
	h.set("name1", "value1");
	w.nextPart(h);
	ostr << "this is part 1";
	h.clear();
	w.nextPart(h);
	ostr << "this is part 2";
	w.close();
	std::string s = ostr.str();	
	assert (s == "--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567\r\n\r\nthis is part 2\r\n--MIME_boundary_01234567--\r\n");
}


void MultipartWriterTest::testBoundary()
{
	std::ostringstream ostr;
	MultipartWriter w(ostr);
	std::string boundary = w.boundary();
	assert (boundary.substr(0, 14) == "MIME_boundary_");
	assert (boundary.length() == 14 + 16);
}


void MultipartWriterTest::setUp()
{
}


void MultipartWriterTest::tearDown()
{
}


CppUnit::Test* MultipartWriterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MultipartWriterTest");

	CppUnit_addTest(pSuite, MultipartWriterTest, testWriteOnePart);
	CppUnit_addTest(pSuite, MultipartWriterTest, testWriteTwoParts);
	CppUnit_addTest(pSuite, MultipartWriterTest, testBoundary);

	return pSuite;
}
