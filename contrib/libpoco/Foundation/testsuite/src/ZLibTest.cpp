//
// ZLibTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ZLibTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ZLibTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/InflatingStream.h"
#include "Poco/DeflatingStream.h"
#include "Poco/MemoryStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/Buffer.h"
#include <sstream>


using Poco::InflatingInputStream;
using Poco::InflatingOutputStream;
using Poco::DeflatingOutputStream;
using Poco::DeflatingInputStream;
using Poco::InflatingStreamBuf;
using Poco::DeflatingStreamBuf;
using Poco::StreamCopier;


ZLibTest::ZLibTest(const std::string& name): CppUnit::TestCase(name)
{
}


ZLibTest::~ZLibTest()
{
}


void ZLibTest::testDeflate1()
{
	std::stringstream buffer;
	DeflatingOutputStream deflater(buffer);
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater.close();
	InflatingInputStream inflater(buffer);
	std::string data;
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
}


void ZLibTest::testDeflate2()
{
	std::stringstream buffer;
	DeflatingOutputStream deflater(buffer);
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater.close();
	std::stringstream buffer2;
	InflatingOutputStream inflater(buffer2);
	StreamCopier::copyStream(buffer, inflater);
	inflater.close();
	std::string data;
	buffer2 >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	buffer2 >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
}


void ZLibTest::testDeflate3()
{
	std::stringstream buffer;
	buffer << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	buffer << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	DeflatingInputStream deflater(buffer);
	std::stringstream buffer2;
	StreamCopier::copyStream(deflater, buffer2);
	std::stringstream buffer3;
	InflatingOutputStream inflater(buffer3);
	StreamCopier::copyStream(buffer2, inflater);
	inflater.close();
	std::string data;
	buffer3 >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	buffer3 >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
}


void ZLibTest::testDeflate4()
{
	Poco::Buffer<char> buffer(1024);
	Poco::MemoryOutputStream ostr(buffer.begin(), static_cast<std::streamsize>(buffer.size()));
	DeflatingOutputStream deflater(ostr, -10, Z_BEST_SPEED);
	std::string data(36828, 'x');
	deflater << data;
	deflater.close();
	Poco::MemoryInputStream istr(buffer.begin(), ostr.charsWritten());
	InflatingInputStream inflater(istr, -10);
	std::string data2;
	inflater >> data2;
	assert (data2 == data);
}


void ZLibTest::testGzip1()
{
	std::stringstream buffer;
	DeflatingOutputStream deflater(buffer, DeflatingStreamBuf::STREAM_GZIP);
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater.close();
	InflatingInputStream inflater(buffer, InflatingStreamBuf::STREAM_GZIP);
	std::string data;
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
}


void ZLibTest::testGzip2()
{
	// created with gzip ("Hello, world!"):
	const unsigned char gzdata[] = 
	{
		0x1f, 0x8b, 0x08, 0x08, 0xb0, 0x73, 0xd0, 0x41, 0x00, 0x03, 0x68, 0x77, 0x00, 0xf3, 0x48, 0xcd, 
		0xc9, 0xc9, 0xd7, 0x51, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x51, 0xe4, 0x02, 0x00, 0x18, 0xa7, 0x55, 
		0x7b, 0x0e, 0x00, 0x00, 0x00, 0x00
	};
	
	std::string gzstr((char*) gzdata, sizeof(gzdata));
	std::istringstream istr(gzstr);
	InflatingInputStream inflater(istr, InflatingStreamBuf::STREAM_GZIP);
	std::string data;
	inflater >> data;
	assert (data == "Hello,");
	inflater >> data;
	assert (data == "world!");	
}


void ZLibTest::testGzip3()
{
	std::stringstream buffer;
	DeflatingOutputStream deflater1(buffer, DeflatingStreamBuf::STREAM_GZIP);
	deflater1 << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater1 << "abcdefabcdefabcdefabcdefabcdefabcdef" << std::endl;
	deflater1.close();
	DeflatingOutputStream deflater2(buffer, DeflatingStreamBuf::STREAM_GZIP);
	deflater2 << "bcdefabcdefabcdefabcdefabcdefabcdefa" << std::endl;
	deflater2 << "bcdefabcdefabcdefabcdefabcdefabcdefa" << std::endl;
	deflater2.close();
	InflatingInputStream inflater(buffer, InflatingStreamBuf::STREAM_GZIP);
	std::string data;
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	inflater >> data;
	assert (data == "abcdefabcdefabcdefabcdefabcdefabcdef");
	data.clear();
	inflater >> data;
	assert (data.empty());
	assert (inflater.eof());
	inflater.reset();
	inflater >> data;
	assert (data == "bcdefabcdefabcdefabcdefabcdefabcdefa");
	inflater >> data;
	assert (data == "bcdefabcdefabcdefabcdefabcdefabcdefa");	
}


void ZLibTest::setUp()
{
}


void ZLibTest::tearDown()
{
}


CppUnit::Test* ZLibTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ZLibTest");

	CppUnit_addTest(pSuite, ZLibTest, testDeflate1);
	CppUnit_addTest(pSuite, ZLibTest, testDeflate2);
	CppUnit_addTest(pSuite, ZLibTest, testDeflate3);
	CppUnit_addTest(pSuite, ZLibTest, testDeflate4);
	CppUnit_addTest(pSuite, ZLibTest, testGzip1);
	CppUnit_addTest(pSuite, ZLibTest, testGzip2);
	CppUnit_addTest(pSuite, ZLibTest, testGzip3);

	return pSuite;
}
