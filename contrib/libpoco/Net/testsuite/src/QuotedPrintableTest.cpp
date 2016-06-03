//
// QuotedPrintableTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/QuotedPrintableTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "QuotedPrintableTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/QuotedPrintableEncoder.h"
#include "Poco/Net/QuotedPrintableDecoder.h"
#include <sstream>


using Poco::Net::QuotedPrintableEncoder;
using Poco::Net::QuotedPrintableDecoder;


QuotedPrintableTest::QuotedPrintableTest(const std::string& name): CppUnit::TestCase(name)
{
}


QuotedPrintableTest::~QuotedPrintableTest()
{
}


void QuotedPrintableTest::testEncode()
{
	std::ostringstream ostr;
	QuotedPrintableEncoder encoder(ostr);
	
	encoder <<
		"Lorem ipsum dolor sit amet, consectetuer adipiscing elit.\r\n"
		"Proin id odio sit amet metus dignissim porttitor. \r\n"
		"Aliquam nulla ipsum, faucibus non, aliquet quis, aliquet id, felis. Proin sodales molestie arcu.\r\n"
		"\t\bSed suscipit, mi in facilisis feugiat, \t   \r\n"
		"\200\201\r\n";
	encoder.close();
	std::string txt = ostr.str();
	assert (txt == "Lorem ipsum dolor sit amet, consectetuer adipiscing elit.\r\n"
	               "Proin id odio sit amet metus dignissim porttitor.=20\r\n"
	               "Aliquam nulla ipsum, faucibus non, aliquet quis, aliquet id, felis. Proin s=\r\n"
	               "odales molestie arcu.\r\n"
	               "\t=08Sed suscipit, mi in facilisis feugiat, \t  =20\r\n"
	               "=80=81\r\n");
}


void QuotedPrintableTest::testDecode()
{
	std::istringstream istr(
		"Lorem ipsum dolor sit amet, consectetuer adipiscing elit.\r\n"
	    "Proin id odio sit amet metus dignissim porttitor.=20\r\n"
	    "Aliquam nulla ipsum, faucibus non, aliquet quis, aliquet id, felis. Proin s=\r\n"
	    "odales molestie arcu.\r\n"
	    "\t=08Sed suscipit, mi in facilisis feugiat, \t  =20\r\n"
	    "=80=81\r\n"
	);
	QuotedPrintableDecoder decoder(istr);
	std::string str;
	int c = decoder.get();
	while (c != -1)
	{
		str += (char) c;
		c = decoder.get();
	}
	assert (str == "Lorem ipsum dolor sit amet, consectetuer adipiscing elit.\r\n"
	               "Proin id odio sit amet metus dignissim porttitor. \r\n"
	               "Aliquam nulla ipsum, faucibus non, aliquet quis, aliquet id, felis. Proin sodales molestie arcu.\r\n"
	               "\t\bSed suscipit, mi in facilisis feugiat, \t   \r\n"
	               "\200\201\r\n");

}


void QuotedPrintableTest::setUp()
{
}


void QuotedPrintableTest::tearDown()
{
}


CppUnit::Test* QuotedPrintableTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("QuotedPrintableTest");

	CppUnit_addTest(pSuite, QuotedPrintableTest, testEncode);
	CppUnit_addTest(pSuite, QuotedPrintableTest, testDecode);

	return pSuite;
}
