//
// LineEndingConverterTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LineEndingConverterTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LineEndingConverterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/LineEndingConverter.h"
#include "Poco/StreamCopier.h"
#include <sstream>


using Poco::LineEnding;
using Poco::InputLineEndingConverter;
using Poco::OutputLineEndingConverter;
using Poco::StreamCopier;


LineEndingConverterTest::LineEndingConverterTest(const std::string& name): CppUnit::TestCase(name)
{
}


LineEndingConverterTest::~LineEndingConverterTest()
{
}


void LineEndingConverterTest::testInputDosToUnix()
{
	std::istringstream input("line1\r\nline2\r\nline3\r\n");
	std::ostringstream output;
	InputLineEndingConverter conv(input, LineEnding::NEWLINE_LF);
	StreamCopier::copyStream(conv, output);
	std::string result = output.str();
	assert (result == "line1\nline2\nline3\n");
}


void LineEndingConverterTest::testInputUnixToDos()
{
	std::istringstream input("line1\nline2\nline3\n");
	std::ostringstream output;
	InputLineEndingConverter conv(input, LineEnding::NEWLINE_CRLF);
	StreamCopier::copyStream(conv, output);
	std::string result = output.str();
	assert (result == "line1\r\nline2\r\nline3\r\n");
}


void LineEndingConverterTest::testInputMacToUnix()
{
	std::istringstream input("line1\rline2\rline3\r");
	std::ostringstream output;
	InputLineEndingConverter conv(input, LineEnding::NEWLINE_LF);
	StreamCopier::copyStream(conv, output);
	std::string result = output.str();
	assert (result == "line1\nline2\nline3\n");
}


void LineEndingConverterTest::testInputRemove()
{
	std::istringstream input("line1\r\nline2\rline3\n");
	std::ostringstream output;
	InputLineEndingConverter conv(input, "");
	StreamCopier::copyStream(conv, output);
	std::string result = output.str();
	assert (result == "line1line2line3");
}


void LineEndingConverterTest::testOutputDosToUnix()
{
	std::ostringstream output;
	OutputLineEndingConverter conv(output, LineEnding::NEWLINE_LF);
	conv << "line1\r\nline2\r\nline3\r\n" << std::flush;
	std::string result = output.str();
	assert (result == "line1\nline2\nline3\n");
}


void LineEndingConverterTest::testOutputUnixToDos()
{
	std::ostringstream output;
	OutputLineEndingConverter conv(output, LineEnding::NEWLINE_CRLF);
	conv << "line1\nline2\nline3\n" << std::flush;
	std::string result = output.str();
	assert (result == "line1\r\nline2\r\nline3\r\n");
}


void LineEndingConverterTest::testOutputMacToUnix()
{
	std::ostringstream output;
	OutputLineEndingConverter conv(output, LineEnding::NEWLINE_LF);
	conv << "line1\rline2\rline3\r" << std::flush;
	std::string result = output.str();
	assert (result == "line1\nline2\nline3\n");
}


void LineEndingConverterTest::testOutputRemove()
{
	std::ostringstream output;
	OutputLineEndingConverter conv(output, "");
	conv << "line1\r\nline2\rline3\n" << std::flush;
	std::string result = output.str();
	assert (result == "line1line2line3");
}


void LineEndingConverterTest::setUp()
{
}


void LineEndingConverterTest::tearDown()
{
}


CppUnit::Test* LineEndingConverterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LineEndingConverterTest");

	CppUnit_addTest(pSuite, LineEndingConverterTest, testInputDosToUnix);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testInputUnixToDos);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testInputMacToUnix);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testInputRemove);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testOutputDosToUnix);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testOutputUnixToDos);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testOutputMacToUnix);
	CppUnit_addTest(pSuite, LineEndingConverterTest, testOutputRemove);

	return pSuite;
}
