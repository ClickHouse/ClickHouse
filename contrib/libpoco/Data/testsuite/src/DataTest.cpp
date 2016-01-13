//
// DataTest.cpp
//
// $Id: //poco/Main/Data/testsuite/src/DataTest.cpp#12 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DataTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/LOBStream.h"
#include "Poco/Data/MetaColumn.h"
#include "Poco/Data/Column.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/SimpleRowFormatter.h"
#include "Poco/Data/DataException.h"
#include "Connector.h"
#include "Poco/BinaryReader.h"
#include "Poco/BinaryWriter.h"
#include "Poco/DateTime.h"
#include "Poco/Types.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Data/DynamicLOB.h"
#include "Poco/Data/DynamicDateTime.h"
#include "Poco/Exception.h"
#include <cstring>
#include <sstream>
#include <iomanip>
#include <set>


using namespace Poco::Data::Keywords;


using Poco::BinaryReader;
using Poco::BinaryWriter;
using Poco::UInt32;
using Poco::Int64;
using Poco::UInt64;
using Poco::DateTime;
using Poco::Dynamic::Var;
using Poco::InvalidAccessException;
using Poco::IllegalStateException;
using Poco::RangeException;
using Poco::NotFoundException;
using Poco::InvalidArgumentException;
using Poco::NotImplementedException;
using Poco::Data::Session;
using Poco::Data::SessionFactory;
using Poco::Data::Statement;
using Poco::Data::NotSupportedException;
using Poco::Data::CLOB;
using Poco::Data::CLOBInputStream;
using Poco::Data::CLOBOutputStream;
using Poco::Data::MetaColumn;
using Poco::Data::Column;
using Poco::Data::Row;
using Poco::Data::SimpleRowFormatter;
using Poco::Data::Date;
using Poco::Data::Time;
using Poco::Data::AbstractExtraction;
using Poco::Data::AbstractExtractionVec;
using Poco::Data::AbstractExtractionVecVec;
using Poco::Data::AbstractBinding;
using Poco::Data::AbstractBindingVec;
using Poco::Data::NotConnectedException;


DataTest::DataTest(const std::string& name): CppUnit::TestCase(name)
{
	Poco::Data::Test::Connector::addToFactory();
}


DataTest::~DataTest()
{
	Poco::Data::Test::Connector::removeFromFactory();
}


void DataTest::testSession()
{
	Session sess(SessionFactory::instance().create("test", "cs"));
	assert ("test" == sess.impl()->connectorName());
	assert (sess.connector() == sess.impl()->connectorName());
	assert ("cs" == sess.impl()->connectionString());
	assert ("test:///cs" == sess.uri());

	assert (sess.getLoginTimeout() == Session::LOGIN_TIMEOUT_DEFAULT);
	sess.setLoginTimeout(123);
	assert (sess.getLoginTimeout() == 123);

	Session sess2(SessionFactory::instance().create("TeSt:///Cs"));
	assert ("test" == sess2.impl()->connectorName());
	assert ("Cs" == sess2.impl()->connectionString());
	assert ("test:///Cs" == sess2.uri());

	sess << "DROP TABLE IF EXISTS Test", now;
	int count;
	sess << "SELECT COUNT(*) FROM PERSON", into(count), now;
	
	std::string str;
	Statement stmt = (sess << "SELECT * FROM Strings", into(str), limit(50));
	stmt.execute();

	sess.close();
	assert (!sess.getFeature("connected"));
	assert (!sess.isConnected());

	try
	{
		stmt.execute(); 
		fail ("must fail");
	} catch (NotConnectedException&) { }

	try
	{
		sess << "SELECT * FROM Strings", now; 
		fail ("must fail");
	} catch (NotConnectedException&) { }

	sess.open();
	assert (sess.getFeature("connected"));
	assert (sess.isConnected());
	
	sess << "SELECT * FROM Strings", now; 
	stmt.execute();

	sess.reconnect();
	assert (sess.getFeature("connected"));
	assert (sess.isConnected());
	
	sess << "SELECT * FROM Strings", now; 
	stmt.execute();
}


void DataTest::testStatementFormatting()
{
	Session sess(SessionFactory::instance().create("test", "cs"));

	Statement stmt = (sess << "SELECT %s%c%s,%d,%u,%f,%s FROM Person WHERE Name LIKE 'Simp%%'", 
		"'",'a',"'",-1, 1u, 1.5, "42", now);
	
	assert ("SELECT 'a',-1,1,1.500000,42 FROM Person WHERE Name LIKE 'Simp%'" == stmt.toString());
}


void DataTest::testFeatures()
{
	Session sess(SessionFactory::instance().create("test", "cs"));
	
	sess.setFeature("f1", true);
	assert (sess.getFeature("f1"));
	assert (sess.getFeature("f2"));
	
	try
	{
		sess.setFeature("f2", false);
	}
	catch (NotImplementedException&)
	{
	}
	
	sess.setFeature("f3", false);
	assert (!sess.getFeature("f2"));
	
	try
	{
		sess.setFeature("f3", true);
	}
	catch (NotImplementedException&)
	{
	}
	
	try
	{
		sess.setFeature("f4", false);
	}
	catch (NotSupportedException&)
	{
	}
}


void DataTest::testProperties()
{
	Session sess(SessionFactory::instance().create("test", "cs"));
		
	sess.setProperty("p1", 1);
	Poco::Any v1 = sess.getProperty("p1");
	assert (Poco::AnyCast<int>(v1) == 1);
	Poco::Any v2 = sess.getProperty("p2");
	assert (Poco::AnyCast<int>(v2) == 1);
	
	try
	{
		sess.setProperty("p2", 2);
	}
	catch (NotImplementedException&)
	{
	}
	
	sess.setProperty("p3", 2);
	v1 = sess.getProperty("p2");
	assert (Poco::AnyCast<int>(v1) == 2);
	
	try
	{
		sess.setProperty("p3", 3);
	}
	catch (NotImplementedException&)
	{
	}
	
	try
	{
		sess.setProperty("p4", 4);
	}
	catch (NotSupportedException&)
	{
	}
}


void DataTest::testLOB()
{
	std::vector<int> vec;
	vec.push_back(0);
	vec.push_back(1);
	vec.push_back(2);
	vec.push_back(3);
	vec.push_back(4);
	vec.push_back(5);
	vec.push_back(6);
	vec.push_back(7);
	vec.push_back(8);
	vec.push_back(9);

	Poco::Data::LOB<int> lobNum1(&vec[0], vec.size());
	assert (lobNum1.size() == vec.size());
	assert (0 == std::memcmp(&vec[0], lobNum1.rawContent(), lobNum1.size() * sizeof(int)));
	assert (*lobNum1.begin() == 0);
	Poco::Data::LOB<int>::Iterator it1 = lobNum1.end();
	assert (*(--it1) == 9);
	
	Poco::Data::LOB<int> lobNum2(lobNum1);
	assert (lobNum2.size() == lobNum1.size());
	assert (lobNum2 == lobNum1);
	lobNum1.swap(lobNum2);
	assert (lobNum2 == lobNum1);
}


void DataTest::testCLOB()
{
	std::string strDigit = "1234567890";
	std::string strAlpha = "abcdefghijklmnopqrstuvwxyz";
	std::vector<char> vecAlpha(strAlpha.begin(), strAlpha.end());
	std::vector<char> vecDigit(strDigit.begin(), strDigit.end());
	
	CLOB blobNumStr(strDigit.c_str(), strDigit.size());
	assert (blobNumStr.size() == strDigit.size());
	assert (0 == std::strncmp(strDigit.c_str(), blobNumStr.rawContent(), blobNumStr.size()));
	assert (*blobNumStr.begin() == '1');
	CLOB::Iterator itNumStr = blobNumStr.end();
	assert (*(--itNumStr) == '0');
	CLOB blobNumVec(vecDigit);
	assert (blobNumVec.size() == vecDigit.size());
	assert (blobNumVec == blobNumStr);
	blobNumVec.swap(blobNumStr);
	assert (blobNumVec.size() == blobNumStr.size());
	assert (blobNumVec == blobNumStr);

	CLOB blobChrStr(strAlpha.c_str(), strAlpha.size());
	CLOB blobChrVec(vecAlpha);
	assert (blobChrStr.size() == strAlpha.size());
	assert (0 == std::strncmp(strAlpha.c_str(), blobChrStr.rawContent(), blobChrStr.size()));
	assert (*blobChrStr.begin() == 'a');
	CLOB::Iterator itChrStr = blobChrStr.end();
	assert (*(--itChrStr) == 'z');
	assert (blobChrStr == blobChrVec);

	blobNumStr.swap(blobChrStr);
	assert (blobNumStr != blobChrStr);
	assert (&blobNumStr != &blobChrStr);
	assert (blobNumStr.content() != blobChrStr.content());
	assert (&blobNumStr.content() != &blobChrStr.content());
	assert (blobNumStr == blobChrVec);

	Poco::Data::swap(blobNumStr, blobChrVec);
	assert (blobNumStr == blobChrVec);
	std::swap(blobNumStr, blobChrVec);
	assert (blobNumStr == blobChrVec);

	assert (blobChrStr != blobNumStr);
	Var vLOB = blobNumStr;
	std::string sss = vLOB.convert<std::string>();
	blobChrStr = CLOB(sss);
	assert (blobChrStr == blobNumStr);

    std::string xyz = "xyz";
	vLOB = xyz;
	blobChrStr = sss = vLOB.convert<std::string>();
	assert (0 == std::strncmp(xyz.c_str(), blobChrStr.rawContent(), blobChrStr.size()));
}


void DataTest::testCLOBStreams()
{
	CLOB blob;
	assert (0 == blob.size());

	CLOBOutputStream bos(blob);
	BinaryWriter bw(bos);

	assert (0 == blob.size());
	writeToCLOB(bw);
	assert (blob.size() > 0);

	CLOBInputStream bis(blob);
	BinaryReader br(bis);

	readFromCLOB(br);
}


void DataTest::writeToCLOB(BinaryWriter& writer)
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
	
	writer << (Int64) -1234567890;
	writer << (UInt64) 1234567890;

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

	writer.write7BitEncoded((UInt64) 100);
	writer.write7BitEncoded((UInt64) 1000);
	writer.write7BitEncoded((UInt64) 10000);
	writer.write7BitEncoded((UInt64) 100000);
	writer.write7BitEncoded((UInt64) 1000000);

	writer.writeRaw("RAW");
}


void DataTest::readFromCLOB(BinaryReader& reader)
{
	bool b = false;
	reader >> b;
	assert (b);
	reader >> b;
	assert (!b);
	
	char c = ' ';
	reader >> c;
	assert (c == 'a');

	short shortv = 0;
	reader >> shortv;
	assert (shortv == -100);

	unsigned short ushortv = 0;
	reader >> ushortv;
	assert (ushortv == 50000);

	int intv = 0;
	reader >> intv;
	assert (intv == -123456);

	unsigned uintv = 0;
	reader >> uintv;
	assert (uintv == 123456);

	long longv = 0;
	reader >> longv;
	assert (longv == -1234567890);

	unsigned long ulongv = 0;
	reader >> ulongv;
	assert (ulongv == 1234567890);

	Int64 int64v = 0;
	reader >> int64v;
	assert (int64v == -1234567890);
	
	UInt64 uint64v = 0;
	reader >> uint64v;
	assert (uint64v == 1234567890);

	float floatv = 0.0;
	reader >> floatv;
	assert (floatv == 1.5);
	
	double doublev = 0.0;
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

	reader.readRaw(3, str);
	assert (str == "RAW");
}


void DataTest::testColumnVector()
{
	MetaColumn mc(0, "mc", MetaColumn::FDT_DOUBLE, 2, 3, true);

	assert (mc.name() == "mc");
	assert (mc.position() == 0);
	assert (mc.length() == 2);
	assert (mc.precision() == 3);
	assert (mc.type() == MetaColumn::FDT_DOUBLE);
	assert (mc.isNullable());

	std::vector<int>* pData = new std::vector<int>;
	pData->push_back(1);
	pData->push_back(2);
	pData->push_back(3);
	pData->push_back(4);
	pData->push_back(5);
	
	Column<std::vector<int> > c(mc, pData);

	assert (c.rowCount() == 5);
	assert (c[0] == 1);
	assert (c[1] == 2);
	assert (c[2] == 3);
	assert (c[3] == 4);
	assert (c[4] == 5);
	assert (c.name() == "mc");
	assert (c.position() == 0);
	assert (c.length() == 2);
	assert (c.precision() == 3);
	assert (c.type() == MetaColumn::FDT_DOUBLE);

	try
	{
		int i; i = c[100]; // to silence gcc
		fail ("must fail");
	}
	catch (RangeException&) { }

	Column<std::vector<int> > c1 = c;

	assert (c1.rowCount() == 5);
	assert (c1[0] == 1);
	assert (c1[1] == 2);
	assert (c1[2] == 3);
	assert (c1[3] == 4);
	assert (c1[4] == 5);

	Column<std::vector<int> > c2(c1);

	assert (c2.rowCount() == 5);
	assert (c2[0] == 1);
	assert (c2[1] == 2);
	assert (c2[2] == 3);
	assert (c2[3] == 4);
	assert (c2[4] == 5);

	std::vector<int> vi;
	vi.assign(c.begin(), c.end());
	assert (vi.size() == 5);
	assert (vi[0] == 1);
	assert (vi[1] == 2);
	assert (vi[2] == 3);
	assert (vi[3] == 4);
	assert (vi[4] == 5);

	c.reset();
	assert (c.rowCount() == 0);
	assert (c1.rowCount() == 0);
	assert (c2.rowCount() == 0);

	std::vector<int>* pV1 = new std::vector<int>;
	pV1->push_back(1);
	pV1->push_back(2);
	pV1->push_back(3);
	pV1->push_back(4);
	pV1->push_back(5);
	std::vector<int>* pV2 = new std::vector<int>;
	pV2->push_back(5);
	pV2->push_back(4);
	pV2->push_back(3);
	pV2->push_back(2);
	pV2->push_back(1);
	Column<std::vector<int> > c3(mc, pV1);
	Column<std::vector<int> > c4(mc, pV2);
	
	Poco::Data::swap(c3, c4);
	assert (c3[0] == 5);
	assert (c3[1] == 4);
	assert (c3[2] == 3);
	assert (c3[3] == 2);
	assert (c3[4] == 1);

	assert (c4[0] == 1);
	assert (c4[1] == 2);
	assert (c4[2] == 3);
	assert (c4[3] == 4);
	assert (c4[4] == 5);

	std::swap(c3, c4);
	assert (c3[0] == 1);
	assert (c3[1] == 2);
	assert (c3[2] == 3);
	assert (c3[3] == 4);
	assert (c3[4] == 5);

	assert (c4[0] == 5);
	assert (c4[1] == 4);
	assert (c4[2] == 3);
	assert (c4[3] == 2);
	assert (c4[4] == 1);
}


void DataTest::testColumnVectorBool()
{
	MetaColumn mc(0, "mc", MetaColumn::FDT_BOOL);

	std::vector<bool>* pData = new std::vector<bool>;
	pData->push_back(true);
	pData->push_back(false);
	pData->push_back(true);
	pData->push_back(false);
	pData->push_back(true);
	
	Column<std::vector<bool> > c(mc, pData);

	assert (c.rowCount() == 5);
	assert (c[0] == true);
	assert (c[1] == false);
	assert (c[2] == true);
	assert (c[3] == false);
	assert (c[4] == true);
	assert (c.type() == MetaColumn::FDT_BOOL);

	try
	{
		bool b; b = c[100]; // to silence gcc
		fail ("must fail");
	}
	catch (RangeException&) { }

	Column<std::vector<bool> > c1 = c;

	assert (c1.rowCount() == 5);
	assert (c1[0] == true);
	assert (c1[1] == false);
	assert (c1[2] == true);
	assert (c1[3] == false);
	assert (c1[4] == true);

	Column<std::vector<bool> > c2(c1);

	assert (c2.rowCount() == 5);
	assert (c2[0] == true);
	assert (c2[1] == false);
	assert (c2[2] == true);
	assert (c2[3] == false);
	assert (c2[4] == true);

	std::vector<bool> vi;
	vi.assign(c.begin(), c.end());
	assert (vi.size() == 5);
	assert (vi[0] == true);
	assert (vi[1] == false);
	assert (vi[2] == true);
	assert (vi[3] == false);
	assert (vi[4] == true);

	c.reset();
	assert (c.rowCount() == 0);
	assert (c1.rowCount() == 0);
	assert (c2.rowCount() == 0);
}


void DataTest::testColumnDeque()
{
	typedef std::deque<int> ContainerType;
	typedef Column<ContainerType> ColumnType;

	MetaColumn mc(0, "mc", MetaColumn::FDT_DOUBLE, 2, 3, true);

	assert (mc.name() == "mc");
	assert (mc.position() == 0);
	assert (mc.length() == 2);
	assert (mc.precision() == 3);
	assert (mc.type() == MetaColumn::FDT_DOUBLE);
	assert (mc.isNullable());

	ContainerType* pData = new ContainerType;
	pData->push_back(1);
	pData->push_back(2);
	pData->push_back(3);
	pData->push_back(4);
	pData->push_back(5);
	
	ColumnType c(mc, pData);

	assert (c.rowCount() == 5);
	assert (c[0] == 1);
	assert (c[1] == 2);
	assert (c[2] == 3);
	assert (c[3] == 4);
	assert (c[4] == 5);
	assert (c.name() == "mc");
	assert (c.position() == 0);
	assert (c.length() == 2);
	assert (c.precision() == 3);
	assert (c.type() == MetaColumn::FDT_DOUBLE);

	try
	{
		int i; i = c[100]; // to silence gcc
		fail ("must fail");
	}
	catch (RangeException&) { }

	ColumnType c1 = c;

	assert (c1.rowCount() == 5);
	assert (c1[0] == 1);
	assert (c1[1] == 2);
	assert (c1[2] == 3);
	assert (c1[3] == 4);
	assert (c1[4] == 5);

	ColumnType c2(c1);

	assert (c2.rowCount() == 5);
	assert (c2[0] == 1);
	assert (c2[1] == 2);
	assert (c2[2] == 3);
	assert (c2[3] == 4);
	assert (c2[4] == 5);

	ContainerType vi;
	vi.assign(c.begin(), c.end());
	assert (vi.size() == 5);
	assert (vi[0] == 1);
	assert (vi[1] == 2);
	assert (vi[2] == 3);
	assert (vi[3] == 4);
	assert (vi[4] == 5);

	c.reset();
	assert (c.rowCount() == 0);
	assert (c1.rowCount() == 0);
	assert (c2.rowCount() == 0);

	ContainerType* pV1 = new ContainerType;
	pV1->push_back(1);
	pV1->push_back(2);
	pV1->push_back(3);
	pV1->push_back(4);
	pV1->push_back(5);
	ContainerType* pV2 = new ContainerType;
	pV2->push_back(5);
	pV2->push_back(4);
	pV2->push_back(3);
	pV2->push_back(2);
	pV2->push_back(1);
	Column<ContainerType> c3(mc, pV1);
	Column<ContainerType> c4(mc, pV2);
	
	Poco::Data::swap(c3, c4);
	assert (c3[0] == 5);
	assert (c3[1] == 4);
	assert (c3[2] == 3);
	assert (c3[3] == 2);
	assert (c3[4] == 1);

	assert (c4[0] == 1);
	assert (c4[1] == 2);
	assert (c4[2] == 3);
	assert (c4[3] == 4);
	assert (c4[4] == 5);

	std::swap(c3, c4);
	assert (c3[0] == 1);
	assert (c3[1] == 2);
	assert (c3[2] == 3);
	assert (c3[3] == 4);
	assert (c3[4] == 5);

	assert (c4[0] == 5);
	assert (c4[1] == 4);
	assert (c4[2] == 3);
	assert (c4[3] == 2);
	assert (c4[4] == 1);
}


void DataTest::testColumnList()
{
	typedef std::list<int> ContainerType;
	typedef Column<ContainerType> ColumnType;

	MetaColumn mc(0, "mc", MetaColumn::FDT_DOUBLE, 2, 3, true);

	assert (mc.name() == "mc");
	assert (mc.position() == 0);
	assert (mc.length() == 2);
	assert (mc.precision() == 3);
	assert (mc.type() == MetaColumn::FDT_DOUBLE);
	assert (mc.isNullable());

	ContainerType* pData = new ContainerType;
	pData->push_back(1);
	pData->push_back(2);
	pData->push_back(3);
	pData->push_back(4);
	pData->push_back(5);
	
	ColumnType c(mc, pData);

	assert (c.rowCount() == 5);
	assert (c[0] == 1);
	assert (c[1] == 2);
	assert (c[2] == 3);
	assert (c[3] == 4);
	assert (c[4] == 5);
	assert (c.name() == "mc");
	assert (c.position() == 0);
	assert (c.length() == 2);
	assert (c.precision() == 3);
	assert (c.type() == MetaColumn::FDT_DOUBLE);

	try
	{
		int i; i = c[100]; // to silence gcc
		fail ("must fail");
	}
	catch (RangeException&) { }

	ColumnType c1 = c;

	assert (c1.rowCount() == 5);
	assert (c1[0] == 1);
	assert (c1[1] == 2);
	assert (c1[2] == 3);
	assert (c1[3] == 4);
	assert (c1[4] == 5);

	ColumnType c2(c1);
	assert (c2.rowCount() == 5);
	assert (c2[0] == 1);
	assert (c2[1] == 2);
	assert (c2[2] == 3);
	assert (c2[3] == 4);
	assert (c2[4] == 5);

	ContainerType vi;
	vi.assign(c.begin(), c.end());
	assert (vi.size() == 5);
	ContainerType::const_iterator it = vi.begin();
	ContainerType::const_iterator end = vi.end();
	for (int i = 1; it != end; ++it, ++i)
		assert (*it == i);

	c.reset();
	assert (c.rowCount() == 0);
	assert (c1.rowCount() == 0);
	assert (c2.rowCount() == 0);

	ContainerType* pV1 = new ContainerType;
	pV1->push_back(1);
	pV1->push_back(2);
	pV1->push_back(3);
	pV1->push_back(4);
	pV1->push_back(5);
	ContainerType* pV2 = new ContainerType;
	pV2->push_back(5);
	pV2->push_back(4);
	pV2->push_back(3);
	pV2->push_back(2);
	pV2->push_back(1);
	Column<ContainerType> c3(mc, pV1);
	Column<ContainerType> c4(mc, pV2);
	
	Poco::Data::swap(c3, c4);
	assert (c3[0] == 5);
	assert (c3[1] == 4);
	assert (c3[2] == 3);
	assert (c3[3] == 2);
	assert (c3[4] == 1);

	assert (c4[0] == 1);
	assert (c4[1] == 2);
	assert (c4[2] == 3);
	assert (c4[3] == 4);
	assert (c4[4] == 5);

	std::swap(c3, c4);
	assert (c3[0] == 1);
	assert (c3[1] == 2);
	assert (c3[2] == 3);
	assert (c3[3] == 4);
	assert (c3[4] == 5);

	assert (c4[0] == 5);
	assert (c4[1] == 4);
	assert (c4[2] == 3);
	assert (c4[3] == 2);
	assert (c4[4] == 1);
}


void DataTest::testRow()
{
	Row row;
	row.append("field0", 0);
	row.append("field1", 1);
	row.append("field2", 2);
	row.append("field3", 3);
	row.append("field4", 4);

	assert (row["field0"] == 0);
	assert (row["field1"] == 1);
	assert (row["field2"] == 2);
	assert (row["field3"] == 3);
	assert (row["field4"] == 4);

	assert (row["FIELD0"] == 0);
	assert (row["FIELD1"] == 1);
	assert (row["FIELD2"] == 2);
	assert (row["FIELD3"] == 3);
	assert (row["FIELD4"] == 4);

	assert (row[0] == 0);
	assert (row[1] == 1);
	assert (row[2] == 2);
	assert (row[3] == 3);
	assert (row[4] == 4);

	try
	{
		int i; i = row[5].convert<int>(); // to silence gcc
		fail ("must fail");
	}catch (RangeException&) {}

	try
	{
		int i; i = row["a bad name"].convert<int>(); // to silence gcc
		fail ("must fail");
	}catch (NotFoundException&) {}

	assert (5 == row.fieldCount());
	assert (row[0] == 0);
	assert (row["field0"] == 0);
	assert (row[1] == 1);
	assert (row["field1"] == 1);
	assert (row[2] == 2);
	assert (row["field2"] == 2);
	assert (row[3] == 3);
	assert (row["field3"] == 3);
	assert (row[4] == 4);
	assert (row["field4"] == 4);

	Row row2;

	row2.append("field0", 5);
	row2.append("field1", 4);
	row2.append("field2", 3);
	row2.append("field3", 2);
	row2.append("field4", 1);

	assert (row != row2);

	Row row3;

	row3.append("field0", 0);
	row3.append("field1", 1);
	row3.append("field2", 2);
	row3.append("field3", 3);
	row3.append("field4", 4);

	assert (row3 == row);
	assert (!(row < row3 | row3 < row));

	Row row4(row3.names());
	try
	{
		row4.set("badfieldname", 0);
		fail ("must fail");
	}catch (NotFoundException&) {}

	try
	{
		row4.set("field1", Var());
		row4.addSortField(1);
		row4.removeSortField(0);
		fail ("must fail - field 1 is empty");
	}
	catch (IllegalStateException&)
	{
		row4.removeSortField(1);
	}

	row4.set("field0", 0);
	row4.set("field1", 1);
	row4.set("field2", 2);
	row4.set("field3", 3);
	row4.set("field4", 4);
	assert (row3 == row4);
	try
	{
		row4.set(5, 0);
		fail ("must fail");
	}catch (RangeException&) {}
	row4.set("field0", 1);
	assert (row3 != row4);
	assert (row3 < row4);
}


void DataTest::testRowSort()
{
	Row row1;
	row1.append("0", 0);
	row1.append("1", 1);
	row1.append("2", 2);
	row1.append("3", 3);
	row1.append("4", 4);

	Row row2;
	row2.append("0", 0);
	row2.append("1", 1);
	row2.append("2", 2);
	row2.append("3", 3);
	row2.append("4", 4);

	std::multiset<Row> rowSet1;
	rowSet1.insert(row1);
	rowSet1.insert(row2);
	std::multiset<Row>::iterator it1 = rowSet1.begin();
	assert (row1 == *it1);
	++it1;
	assert (row2 == *it1);

	Row row3;
	row3.append("0", 1);
	row3.append("1", 1);
	row3.append("2", 2);
	row3.append("3", 3);
	row3.append("4", 4);

	Row row4;
	row4.append("0", 0);
	row4.append("1", 1);
	row4.append("2", 2);
	row4.append("3", 3);
	row4.append("4", 4);

	std::set<Row> rowSet2;
	rowSet2.insert(row4);
	rowSet2.insert(row3);
	std::set<Row>::iterator it2 = rowSet2.begin();
	assert (row4 == *it2);
	++it2;
	assert (row3 == *it2);

	Row row5;
	row5.append("0", 2);
	row5.append("1", 2);
	row5.append("2", 0);
	row5.append("3", 3);
	row5.append("4", 4);
	row5.addSortField("1");

	Row row6;
	row6.append("0", 1);
	row6.append("1", 0);
	row6.append("2", 1);
	row6.append("3", 3);
	row6.append("4", 4);
	row6.addSortField("1");

	Row row7;
	row7.append("0", 0);
	row7.append("1", 1);
	row7.append("2", 2);
	row7.append("3", 3);
	row7.append("4", 4);

	std::set<Row> rowSet3;
	rowSet3.insert(row5);
	rowSet3.insert(row6);
	try
	{
		rowSet3.insert(row7);//has no same sort criteria
		fail ("must fail");
	} catch (InvalidAccessException&) {}

	row7.addSortField("1");
	testRowStrictWeak(row7, row6, row5);
	rowSet3.insert(row7);

	std::set<Row>::iterator it3 = rowSet3.begin();
	assert (row7 == *it3);
	++it3;
	assert (row6 == *it3);
	++it3;
	assert (row5 == *it3);

	row5.replaceSortField("0", "2");
	row6.replaceSortField("0", "2");
	row7.replaceSortField("0", "2");

	rowSet3.clear();
	rowSet3.insert(row7);
	rowSet3.insert(row6);
	rowSet3.insert(row5);

	it3 = rowSet3.begin();
	assert (row5 == *it3);
	++it3;
	assert (row6 == *it3);
	++it3;
	assert (row7 == *it3);

	row5.resetSort();
	row6.resetSort();
	row7.resetSort();

	rowSet3.clear();
	rowSet3.insert(row5);
	rowSet3.insert(row6);
	rowSet3.insert(row7);

	it3 = rowSet3.begin();
	assert (row7 == *it3);
	++it3;
	assert (row6 == *it3);
	++it3;
	assert (row5 == *it3);

	Row row8;
	row8.append("0", "2");
	row8.append("1", "2");
	row8.append("2", "0");
	row8.append("3", "3");
	row8.append("4", "4");
	row8.addSortField("1");

	Row row9;
	row9.append("0", "1");
	row9.append("1", "0");
	row9.append("2", "1");
	row9.append("3", "3");
	row9.append("4", "4");
	row9.addSortField("1");

	Row row10;
	row10.append("0", "0");
	row10.append("1", "1");
	row10.append("2", "2");
	row10.append("3", "3");
	row10.append("4", "4");
	row10.addSortField("1");

	testRowStrictWeak(row10, row9, row8);


	Row row11;
	row11.append("0", 2.5);
	row11.append("1", 2.5);
	row11.append("2", 0.5);
	row11.append("3", 3.5);
	row11.append("4", 4.5);
	row11.addSortField("1");

	Row row12;
	row12.append("0", 1.5);
	row12.append("1", 0.5);
	row12.append("2", 1.5);
	row12.append("3", 3.5);
	row12.append("4", 4.5);
	row12.addSortField("1");

	Row row13;
	row13.append("0", 0.5);
	row13.append("1", 1.5);
	row13.append("2", 2.5);
	row13.append("3", 3.5);
	row13.append("4", 4.5);
	row13.addSortField("1");

	testRowStrictWeak(row13, row12, row11);
}


void DataTest::testRowStrictWeak(const Row& row1, const Row& row2, const Row& row3)
{
	assert (row1 < row2 && !(row2 < row1)); // antisymmetric
	assert (row1 < row2 && row2 < row3 && row1 < row3); // transitive
	assert (!(row1 < row1)); // irreflexive
}


void DataTest::testRowFormat()
{
	Row row1;
	row1.append("field0", 0);
	row1.append("field1", 1);
	row1.append("field2", 2);
	row1.append("field3", 3);
	row1.append("field4", 4);

	SimpleRowFormatter rf;
	std::streamsize sz = rf.getColumnWidth();
	std::streamsize sp = rf.getSpacing();

	std::string line(std::string::size_type(sz * 5 + sp * 4), '-');
	std::string spacer(sp, ' ');
	std::ostringstream os;
	os << std::left 
		<< std::setw(sz) << "field0"
		<< spacer
		<< std::setw(sz) << "field1"
		<< spacer
		<< std::setw(sz) << "field2"
		<< spacer
		<< std::setw(sz) << "field3"
		<< spacer
		<< std::setw(sz) << "field4" << std::endl 
		<< line << std::endl;
	assert (row1.namesToString() == os.str());

	os.str("");
	os << std::right 
		<< std::setw(sz) << "0"
		<< spacer
		<< std::setw(sz) << "1"
		<< spacer
		<< std::setw(sz) << "2"
		<< spacer
		<< std::setw(sz) << "3"
		<< spacer
		<< std::setw(sz) << "4" << std::endl;
	assert (row1.valuesToString() == os.str());
}


void DataTest::testDateAndTime()
{
	DateTime dt;
	Date d(dt);
	Time t(dt);

	assert (dt.year() == d.year());
	assert (dt.month() == d.month());
	assert (dt.day() == d.day());

	assert (dt.hour() == t.hour());
	assert (dt.minute() == t.minute());
	assert (dt.second() == t.second());
	
	Date d1(2007, 6, 15);
	d1.assign(d.year() - 1, d.month(), d.day());
	assert (d1 < d); assert (d1 != d);

	d1.assign(d.year() - 1, 12, d.day());
	assert (d1 < d); assert (d1 != d);

	if (d.day() > 1)
	{
		d1.assign(d.year(), d.month(), d.day() - 1);
		assert (d1 < d); assert (d1 != d);
	}

	d1.assign(d.year() + 1, d.month(), d.day());
	assert (d1 > d); assert (d1 != d);
	
	d1.assign(d.year() + 1, 1, d.day());
	assert (d1 > d); assert (d1 != d);

	if (d.day() < dt.daysOfMonth(dt.year(), dt.month()))
	{
		d1.assign(d.year(), d.month(), d.day() + 1);
		assert (d1 > d); assert (d1 != d);
	}
	
	d1.assign(d.year(), d.month(), d.day());
	assert (d1 == d);

	try { d1.assign(-1, 1, 1); fail ("must fail"); }
	catch (InvalidArgumentException&) { }
	try { d1.assign(1, 0, 1); fail ("must fail"); }
	catch (InvalidArgumentException&) { }
	try { d1.assign(1, 1, 0); fail ("must fail"); }
	catch (InvalidArgumentException&) { }

	Time t1(12, 30, 15);
	
	if (t.hour() > 1)
	{
		t1.assign(t.hour() - 1, t.minute(), t.second());
		assert (t1 < t); assert (t1 != t);
	}

	if (t.minute() > 1)
	{
		t1.assign(t.hour(), t.minute() - 1, t.second());
		assert (t1 < t); assert (t1 != t);
	}
	
	if (t.second() > 1)
	{
		t1.assign(t.hour(), t.minute(), t.second() - 1);
		assert (t1 < t); assert (t1 != t);
	}

	if (t.hour() < 23) 
	{
		t1.assign(t.hour() + 1, t.minute(), t.second());
		assert (t1 > t); assert (t1 != t);
	}

	if (t.minute() < 59)
	{
		t1.assign(t.hour(), t.minute() + 1, t.second());
		assert (t1 > t); assert (t1 != t);
	}

	if (t.second() < 59)
	{
		t1.assign(t.hour(), t.minute(), t.second() + 1);
		assert (t1 > t); assert (t1 != t);
	}

	t1.assign(t.hour(), t.minute(), t.second());
	assert (t1 == t);

	try { t1.assign(-1, 0, 0); fail ("must fail"); }
	catch (InvalidArgumentException&) { }
	try { t1.assign(0, -1, 0); fail ("must fail"); }
	catch (InvalidArgumentException&) { }
	try { t1.assign(0, 0, -1); fail ("must fail"); }
	catch (InvalidArgumentException&) { }

	d1 = dt;
	assert (d1 == dt);

	t1 = dt;
	assert (t1 == dt);

	d.assign(2007, 6, 15);
	d1.assign(2007, 6, 16);
	assert (d != d1);
	Var vDate = d;
	d1 = vDate;
	assert (d == d1);

	t.assign(12, 30, 15);
	t1.assign(12, 30, 16);
	assert (t != t1);
	Var vTime = t;
	t1 = vTime;
	assert (t == t1);
}


void DataTest::testExternalBindingAndExtraction()
{
	Session tmp (Poco::Data::Test::Connector::KEY, "dummy.db");

	int i;
	AbstractExtraction::Ptr pExt1 = into(i);
	AbstractExtraction::Ptr pExt2 = into(i);
	assert (1 == pExt1.referenceCount());
	assert (1 == pExt2.referenceCount());
	{
		Statement stmt(tmp);
		stmt.addExtract(pExt1);
		assert (2 == pExt1.referenceCount());
	}
	assert (1 == pExt1.referenceCount());
	assert (1 == pExt2.referenceCount());

	AbstractBinding::Ptr pBind1 = use(i, "mybind1");
	AbstractBinding::Ptr pBind2 = use(i, "mybind2");
	AbstractBinding::Ptr pBind3 = use(i, "mybind3");
	assert (1 == pBind1.referenceCount());
	assert (1 == pBind2.referenceCount());
	assert (1 == pBind3.referenceCount());
	{
		Statement stmt(tmp);
		stmt.addBind(pBind1);
		assert (2 == pBind1.referenceCount());
		stmt.removeBind(pBind1->name());
		assert (1 == pBind1.referenceCount());
		stmt.addBind(pBind2);
		assert (2 == pBind2.referenceCount());
		stmt.addBind(pBind3);
		assert (2 == pBind3.referenceCount());

		try { stmt.removeBind("a bad name"); fail("must fail"); }
		catch (NotFoundException&) { }
	}
	assert (1 == pBind1.referenceCount());
	assert (1 == pBind2.referenceCount());
	assert (1 == pBind3.referenceCount());
}


void DataTest::setUp()
{
}


void DataTest::tearDown()
{
}


CppUnit::Test* DataTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DataTest");

	CppUnit_addTest(pSuite, DataTest, testSession);
	CppUnit_addTest(pSuite, DataTest, testStatementFormatting);
	CppUnit_addTest(pSuite, DataTest, testFeatures);
	CppUnit_addTest(pSuite, DataTest, testProperties);
	CppUnit_addTest(pSuite, DataTest, testLOB);
	CppUnit_addTest(pSuite, DataTest, testCLOB);
	CppUnit_addTest(pSuite, DataTest, testCLOBStreams);
	CppUnit_addTest(pSuite, DataTest, testColumnVector);
	CppUnit_addTest(pSuite, DataTest, testColumnVectorBool);
	CppUnit_addTest(pSuite, DataTest, testColumnDeque);
	CppUnit_addTest(pSuite, DataTest, testColumnList);
	CppUnit_addTest(pSuite, DataTest, testRow);
	CppUnit_addTest(pSuite, DataTest, testRowSort);
	CppUnit_addTest(pSuite, DataTest, testRowFormat);
	CppUnit_addTest(pSuite, DataTest, testDateAndTime);
	CppUnit_addTest(pSuite, DataTest, testExternalBindingAndExtraction);

	return pSuite;
}
