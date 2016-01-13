//
// MultipartReaderTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MultipartReaderTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MultipartReaderTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MultipartReader.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/NetException.h"
#include <sstream>


using Poco::Net::MultipartReader;
using Poco::Net::MessageHeader;
using Poco::Net::MultipartException;


MultipartReaderTest::MultipartReaderTest(const std::string& name): CppUnit::TestCase(name)
{
}


MultipartReaderTest::~MultipartReaderTest()
{
}


void MultipartReaderTest::testReadOnePart()
{
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.boundary() == "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (!r.hasNextPart());
	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testReadTwoParts()
{
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567\r\n\r\nthis is part 2\r\n\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (r.hasNextPart());
	r.nextPart(h);
	assert (h.empty());
	std::istream& ii = r.stream();
	part.clear();
	ch = ii.get();
	while (ch >= 0)
	{
		part += (char) ch;
		ch = ii.get();
	}
	assert (part == "this is part 2\r\n");

	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testReadEmptyLines()
{
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is\r\npart 1\r\n\r\n--MIME_boundary_01234567\r\n\r\nthis\r\n\r\nis part 2\r\n\r\n\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is\r\npart 1\r\n");
	assert (r.hasNextPart());
	r.nextPart(h);
	assert (h.empty());
	std::istream& ii = r.stream();
	part.clear();
	ch = ii.get();
	while (ch >= 0)
	{
		part += (char) ch;
		ch = ii.get();
	}
	assert (part == "this\r\n\r\nis part 2\r\n\r\n");

	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testReadLongPart()
{
	std::string longPart(3000, 'X');
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\n");
	s.append(longPart);
	s.append("\r\n--MIME_boundary_01234567\r\n\r\nthis is part 2\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == longPart);
	assert (r.hasNextPart());
	r.nextPart(h);
	assert (h.empty());
	std::istream& ii = r.stream();
	part.clear();
	ch = ii.get();
	while (ch >= 0)
	{
		part += (char) ch;
		ch = ii.get();
	}
	assert (part == "this is part 2");

	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testGuessBoundary()
{
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr);
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (r.boundary() == "MIME_boundary_01234567");
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (!r.hasNextPart());
	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testPreamble()
{
	std::string s("this is the\r\npreamble\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (!r.hasNextPart());
	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testBadBoundary()
{
	std::string s("\r\n--MIME_boundary_01234567\r\nname1: value1\r\n\r\nthis is part 1\r\n--MIME_boundary_01234567--\r\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_7654321");
	assert (r.hasNextPart());
	MessageHeader h;
	try
	{
		r.nextPart(h);
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testRobustness()
{
	std::string s("--MIME_boundary_01234567\rname1: value1\r\n\nthis is part 1\n--MIME_boundary_01234567--");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (!r.hasNextPart());
	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::testUnixLineEnds()
{
	std::string s("\n--MIME_boundary_01234567\nname1: value1\n\nthis is part 1\n--MIME_boundary_01234567\n\nthis is part 2\n\n--MIME_boundary_01234567--\n");
	std::istringstream istr(s);
	MultipartReader r(istr, "MIME_boundary_01234567");
	assert (r.hasNextPart());
	MessageHeader h;
	r.nextPart(h);
	assert (h.size() == 1);
	assert (h["name1"] == "value1");
	std::istream& i = r.stream();
	int ch = i.get();
	std::string part;
	while (ch >= 0)
	{
		part += (char) ch;
		ch = i.get();
	}
	assert (part == "this is part 1");
	assert (r.hasNextPart());
	r.nextPart(h);
	assert (h.empty());
	std::istream& ii = r.stream();
	part.clear();
	ch = ii.get();
	while (ch >= 0)
	{
		part += (char) ch;
		ch = ii.get();
	}
	assert (part == "this is part 2\n");

	try
	{
		r.nextPart(h);
		fail("no more parts - must throw");
	}
	catch (MultipartException&)
	{
	}
}


void MultipartReaderTest::setUp()
{
}


void MultipartReaderTest::tearDown()
{
}


CppUnit::Test* MultipartReaderTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MultipartReaderTest");

	CppUnit_addTest(pSuite, MultipartReaderTest, testReadOnePart);
	CppUnit_addTest(pSuite, MultipartReaderTest, testReadTwoParts);
	CppUnit_addTest(pSuite, MultipartReaderTest, testReadEmptyLines);
	CppUnit_addTest(pSuite, MultipartReaderTest, testReadLongPart);
	CppUnit_addTest(pSuite, MultipartReaderTest, testGuessBoundary);
	CppUnit_addTest(pSuite, MultipartReaderTest, testPreamble);
	CppUnit_addTest(pSuite, MultipartReaderTest, testBadBoundary);
	CppUnit_addTest(pSuite, MultipartReaderTest, testRobustness);
	CppUnit_addTest(pSuite, MultipartReaderTest, testUnixLineEnds);

	return pSuite;
}
