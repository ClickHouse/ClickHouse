//
// FIFOBufferStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FIFOBufferStreamTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FIFOBufferStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/FIFOBuffer.h"
#include "Poco/FIFOBufferStream.h"
#include "Poco/Delegate.h"


using Poco::FIFOBuffer;
using Poco::FIFOBufferStream;
using Poco::delegate;


FIFOBufferStreamTest::FIFOBufferStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


FIFOBufferStreamTest::~FIFOBufferStreamTest()
{
}


void FIFOBufferStreamTest::testInput()
{
	const char* data = "This is a test";
	FIFOBuffer fb1(data, 14);
	FIFOBufferStream str1(fb1);
	assert (str1.rdbuf()->fifoBuffer().isFull());

	int c = str1.get();
	assert (c == 'T');
	c = str1.get();
	assert (c == 'h');
	
	std::string str;
	str1 >> str;
	assert (str == "is");
	
	char buffer[32];
	str1.read(buffer, sizeof(buffer));
	assert (str1.gcount() == 10);
	buffer[str1.gcount()] = 0;
	assert (std::string(" is a test") == buffer);
	
	const char* data2 = "123";
	FIFOBufferStream str2(data2, 3);

	c = str2.get();
	assert (c == '1');
	assert (str2.good());
	c = str2.get();
	assert (c == '2');
	str2.unget();
	c = str2.get();
	assert (c == '2');
	assert (str2.good());
	c = str2.get();
	assert (c == '3');
	assert (str2.good());
	c = str2.get();
	assert (c == -1);
	assert (str2.eof());
}


void FIFOBufferStreamTest::testOutput()
{
	char output[64];
	FIFOBufferStream iostr1(output, 64);
	iostr1 << "This is a test " << 42 << std::ends << std::flush;
	assert (std::string("This is a test 42") == output);
}


void FIFOBufferStreamTest::testNotify()
{
	FIFOBuffer fb(18);
	FIFOBufferStream iostr(fb);
	assert (iostr.rdbuf()->fifoBuffer().isEmpty());

	assert (0 == _readableToNot);
	assert (0 == _notToReadable);
	assert (0 == _writableToNot);
	assert (0 == _notToWritable);

	iostr.readable += delegate(this, &FIFOBufferStreamTest::onReadable);
	iostr.writable += delegate(this, &FIFOBufferStreamTest::onWritable);

	iostr << "This is a test " << 42 << std::ends << std::flush;
	assert (iostr.rdbuf()->fifoBuffer().isFull());

	assert (0 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (0 == _notToWritable);

	char input[64];
	iostr >> input;
	assert (std::string("This") == input);
	assert (iostr.rdbuf()->fifoBuffer().isEmpty());
	
	assert (1 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (1 == _notToWritable);

	iostr >> input;
	assert (std::string("is") == input);

	assert (1 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (1 == _notToWritable);

	iostr >> input;
	assert (std::string("a") == input);

	assert (1 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (1 == _notToWritable);

	iostr >> input;
	assert (std::string("test") == input);

	assert (1 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (1 == _notToWritable);

	iostr >> input;
	assert (std::string("42") == input);

	assert (1 == _readableToNot);
	assert (1 == _notToReadable);
	assert (1 == _writableToNot);
	assert (1 == _notToWritable);

	iostr.clear();
	assert (iostr.good());
	iostr << "This is a test " << 42 << std::ends << std::flush;
	assert (iostr.rdbuf()->fifoBuffer().isFull());

	assert (1 == _readableToNot);
	assert (2 == _notToReadable);
	assert (2 == _writableToNot);
	assert (1 == _notToWritable);

	iostr.readable -= delegate(this, &FIFOBufferStreamTest::onReadable);
	iostr.writable -= delegate(this, &FIFOBufferStreamTest::onWritable);
}


void FIFOBufferStreamTest::onReadable(bool& b)
{
	if (b) ++_notToReadable;
	else ++_readableToNot;
};


void FIFOBufferStreamTest::onWritable(bool& b)
{
	if (b) ++_notToWritable;
	else ++_writableToNot;
}


void FIFOBufferStreamTest::setUp()
{
	_readableToNot = 0;
	_notToReadable = 0;
	_writableToNot = 0;
	_notToWritable = 0;
}


void FIFOBufferStreamTest::tearDown()
{
}


CppUnit::Test* FIFOBufferStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FIFOBufferStreamTest");

	CppUnit_addTest(pSuite, FIFOBufferStreamTest, testInput);
	CppUnit_addTest(pSuite, FIFOBufferStreamTest, testOutput);
	CppUnit_addTest(pSuite, FIFOBufferStreamTest, testNotify);

	return pSuite;
}
