//
// PartialStreamTest.cpp
//
// $Id: //poco/1.4/Zip/testsuite/src/PartialStreamTest.cpp#1 $
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PartialStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Zip/PartialStream.h"
#include <sstream>


using namespace Poco::Zip;


PartialStreamTest::PartialStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


PartialStreamTest::~PartialStreamTest()
{
}


void PartialStreamTest::testReading()
{
	std::string message("some dummy message !");
	std::string prefix("pre ");
	std::string postfix(" post");
	std::string result(prefix+message+postfix);
	std::istringstream istr(message);
	PartialInputStream in(istr, 0, static_cast<std::streamoff>(message.length()), true, prefix, postfix);
	char buf[124];
	in.read(buf, 124);
	std::string res(buf, static_cast<std::string::size_type>(in.gcount()));
	assert (res == result);
}


void PartialStreamTest::testWriting()
{
	std::string prefix("X");
	std::string message("some test message");
	std::string postfix("YYY");
	std::string result(prefix+message+postfix);
	std::ostringstream ostr;
	PartialOutputStream out(ostr, prefix.size(), postfix.size());
	out.write(result.c_str(), static_cast<std::streamsize>(result.length()));
	assert (out.good());
	out.close();
	std::string res (ostr.str());
	assert (out.bytesWritten() == message.size());
	assert (message == res);
}


void PartialStreamTest::testWritingZero()
{
	std::string prefix("X");
	std::string message;
	std::string postfix("YYY");
	std::string result(prefix+message+postfix);
	std::ostringstream ostr;
	PartialOutputStream out(ostr, prefix.size(), postfix.size());
	out.write(result.c_str(), static_cast<std::streamsize>(result.length()));
	assert (out.good());
	out.close();
	std::string res (ostr.str());
	assert (out.bytesWritten() == message.size());
	assert (message == res);
}


void PartialStreamTest::testWritingOne()
{
	std::string prefix("X");
	std::string message("a");
	std::string postfix("YYY");
	std::string result(prefix+message+postfix);
	std::ostringstream ostr;
	PartialOutputStream out(ostr, prefix.size(), postfix.size());
	out.write(result.c_str(), static_cast<std::streamsize>(result.length()));
	assert (out.good());
	out.close();
	std::string res (ostr.str());
	assert (out.bytesWritten() == message.size());
	assert (message == res);
}


void PartialStreamTest::setUp()
{
}


void PartialStreamTest::tearDown()
{
}


CppUnit::Test* PartialStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("PartialStreamTest");

	CppUnit_addTest(pSuite, PartialStreamTest, testReading);
	CppUnit_addTest(pSuite, PartialStreamTest, testWriting);
	CppUnit_addTest(pSuite, PartialStreamTest, testWritingZero);
	CppUnit_addTest(pSuite, PartialStreamTest, testWritingOne);

	return pSuite;
}
