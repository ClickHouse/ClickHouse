//
// BinaryReaderWriterTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/BinaryReaderWriterTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "BinaryReaderWriterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/BinaryWriter.h"
#include "Poco/BinaryReader.h"
#include "Poco/Buffer.h"
#include <sstream>


using Poco::BinaryWriter;
using Poco::MemoryBinaryWriter;
using Poco::BinaryReader;
using Poco::MemoryBinaryReader;
using Poco::Buffer;
using Poco::Int32;
using Poco::UInt32;
using Poco::Int64;
using Poco::UInt64;


BinaryReaderWriterTest::BinaryReaderWriterTest(const std::string& name): CppUnit::TestCase(name)
{
}


BinaryReaderWriterTest::~BinaryReaderWriterTest()
{
}


void BinaryReaderWriterTest::testNative()
{
	std::stringstream sstream;
	BinaryWriter writer(sstream);
	BinaryReader reader(sstream);
	write(writer);
	read(reader);
}


void BinaryReaderWriterTest::testBigEndian()
{
	std::stringstream sstream;
	BinaryWriter writer(sstream, BinaryWriter::BIG_ENDIAN_BYTE_ORDER);
	BinaryReader reader(sstream, BinaryReader::UNSPECIFIED_BYTE_ORDER);
	assert (writer.byteOrder() == BinaryWriter::BIG_ENDIAN_BYTE_ORDER);
	writer.writeBOM();
	write(writer);
	reader.readBOM();
	assert (reader.byteOrder() == BinaryReader::BIG_ENDIAN_BYTE_ORDER);
	read(reader);
}


void BinaryReaderWriterTest::testLittleEndian()
{
	std::stringstream sstream;
	BinaryWriter writer(sstream, BinaryWriter::LITTLE_ENDIAN_BYTE_ORDER);
	BinaryReader reader(sstream, BinaryReader::UNSPECIFIED_BYTE_ORDER);
	assert (writer.byteOrder() == BinaryWriter::LITTLE_ENDIAN_BYTE_ORDER);
	writer.writeBOM();
	write(writer);
	reader.readBOM();
	assert (reader.byteOrder() == BinaryReader::LITTLE_ENDIAN_BYTE_ORDER);
	read(reader);
}


void BinaryReaderWriterTest::write(BinaryWriter& writer)
{
	writer << true;
	writer << false;
	writer << 'a';
	writer << (short) -100;
	writer << (unsigned short) 50000;
	writer << -123456;
	writer << (unsigned) 123456;
	writer << (long) -1234567890;
	writer << (unsigned long) 1234567890;
	
#if defined(POCO_HAVE_INT64)
	writer << (Int64) -1234567890;
	writer << (UInt64) 1234567890;
#endif

	writer << (float) 1.5;
	writer << (double) -1.5;
	
	writer << "foo";
	writer << "";
	
	writer << std::string("bar");
	writer << std::string();
	
	writer.write7BitEncoded((UInt32) 100);
	writer.write7BitEncoded((UInt32) 1000);
	writer.write7BitEncoded((UInt32) 10000);
	writer.write7BitEncoded((UInt32) 100000);
	writer.write7BitEncoded((UInt32) 1000000);

#if defined(POCO_HAVE_INT64)
	writer.write7BitEncoded((UInt64) 100);
	writer.write7BitEncoded((UInt64) 1000);
	writer.write7BitEncoded((UInt64) 10000);
	writer.write7BitEncoded((UInt64) 100000);
	writer.write7BitEncoded((UInt64) 1000000);
#endif
	
	std::vector<int> vec;
	vec.push_back(1);
	vec.push_back(2);
	vec.push_back(3);
	writer << vec;

	writer.writeRaw("RAW");
}


void BinaryReaderWriterTest::read(BinaryReader& reader)
{
	bool b;
	reader >> b;
	assert (b);
	reader >> b;
	assert (!b);
	
	char c;
	reader >> c;
	assert (c == 'a');

	short shortv;
	reader >> shortv;
	assert (shortv == -100);

	unsigned short ushortv;
	reader >> ushortv;
	assert (ushortv == 50000);

	int intv;
	reader >> intv;
	assert (intv == -123456);

	unsigned uintv;
	reader >> uintv;
	assert (uintv == 123456);

	long longv;
	reader >> longv;
	assert (longv == -1234567890);

	unsigned long ulongv;
	reader >> ulongv;
	assert (ulongv == 1234567890);

#if defined(POCO_HAVE_INT64)
	Int64 int64v;
	reader >> int64v;
	assert (int64v == -1234567890);
	
	UInt64 uint64v;
	reader >> uint64v;
	assert (uint64v == 1234567890);
#endif

	float floatv;
	reader >> floatv;
	assert (floatv == 1.5);
	
	double doublev;
	reader >> doublev;
	assert (doublev == -1.5);
	
	std::string str;
	reader >> str;
	assert (str == "foo");
	reader >> str;
	assert (str == "");
	reader >> str;
	assert (str == "bar");
	reader >> str;
	assert (str == "");
	
	UInt32 uint32v;
	reader.read7BitEncoded(uint32v);
	assert (uint32v == 100);
	reader.read7BitEncoded(uint32v);
	assert (uint32v == 1000);
	reader.read7BitEncoded(uint32v);
	assert (uint32v == 10000);
	reader.read7BitEncoded(uint32v);
	assert (uint32v == 100000);
	reader.read7BitEncoded(uint32v);
	assert (uint32v == 1000000);

#if defined(POCO_HAVE_INT64)
	reader.read7BitEncoded(uint64v);
	assert (uint64v == 100);
	reader.read7BitEncoded(uint64v);
	assert (uint64v == 1000);
	reader.read7BitEncoded(uint64v);
	assert (uint64v == 10000);
	reader.read7BitEncoded(uint64v);
	assert (uint64v == 100000);
	reader.read7BitEncoded(uint64v);
	assert (uint64v == 1000000);
#endif

	std::vector<int> vec;
	reader >> vec;
	assert (vec.size() == 3);
	assert (vec[0] == 1);
	assert (vec[1] == 2);
	assert (vec[2] == 3);

	reader.readRaw(3, str);
	assert (str == "RAW");
}


void BinaryReaderWriterTest::testWrappers()
{
	bool b = false; char c = '0'; int i = 0;
	Buffer<char> buf(2 * sizeof(bool) + sizeof(char) + 2 * sizeof(int));

	MemoryBinaryWriter writer(buf);
	writer << true;
	writer << false;
	writer << 'a';
	writer << 1;
	writer << -1;

	MemoryBinaryReader reader(writer.data());
	reader >> b; assert (b);
	reader >> b; assert (!b);
	reader >> c; assert ('a' == c);
	assert(reader.available() == sizeof(i) * 2);
	reader >> i; assert (1 == i);
	assert(reader.available() == sizeof(i));
	reader >> i; assert (-1 == i);
	assert(reader.available() == 0);

	reader.setExceptions(std::istream::eofbit);
	try
	{
		reader >> i;
		fail ("must throw on EOF");
	} catch(std::exception&) { }
}


void BinaryReaderWriterTest::setUp()
{
}


void BinaryReaderWriterTest::tearDown()
{
}


CppUnit::Test* BinaryReaderWriterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("BinaryReaderWriterTest");

	CppUnit_addTest(pSuite, BinaryReaderWriterTest, testNative);
	CppUnit_addTest(pSuite, BinaryReaderWriterTest, testBigEndian);
	CppUnit_addTest(pSuite, BinaryReaderWriterTest, testLittleEndian);
	CppUnit_addTest(pSuite, BinaryReaderWriterTest, testWrappers);

	return pSuite;
}
