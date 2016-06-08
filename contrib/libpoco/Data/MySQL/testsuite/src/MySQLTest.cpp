//
// MySQLTest.cpp
//
// $Id: //poco/1.4/Data/MySQL/testsuite/src/MySQLTest.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MySQLTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Format.h"
#include "Poco/Tuple.h"
#include "Poco/NamedTuple.h"
#include "Poco/Exception.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/MySQL/Utility.h"
#include "Poco/Data/MySQL/MySQLException.h"
#include "Poco/Nullable.h"
#include "Poco/Data/DataException.h"
#include <iostream>

using namespace Poco::Data;
using namespace Poco::Data::Keywords;
using Poco::Data::MySQL::ConnectionException;
using Poco::Data::MySQL::Utility;
using Poco::Data::MySQL::StatementException;
using Poco::format;
using Poco::NotFoundException;
using Poco::Int32;
using Poco::Nullable;
using Poco::Tuple;
using Poco::NamedTuple;

Poco::SharedPtr<Poco::Data::Session> MySQLTest::_pSession = 0;
Poco::SharedPtr<SQLExecutor> MySQLTest::_pExecutor = 0;

//
// Parameters for barebone-test
#define MYSQL_USER "root"
#define MYSQL_PWD  "poco"
#define MYSQL_HOST "localhost"
#define MYSQL_PORT 3306
#define MYSQL_DB   "pocotestdb"

//
// Connection string
std::string MySQLTest::_dbConnString = "host=" MYSQL_HOST
	";user=" MYSQL_USER
	";password=" MYSQL_PWD
	";db=" MYSQL_DB
	";compress=true"
	";auto-reconnect=true"
	";secure-auth=true";


MySQLTest::MySQLTest(const std::string& name):
	CppUnit::TestCase(name)
{
	MySQL::Connector::registerConnector();
}


MySQLTest::~MySQLTest()
{
	MySQL::Connector::unregisterConnector();
}


void MySQLTest::dbInfo(Session& session)
{
	std::cout << "Server Info: " << Utility::serverInfo(session) << std::endl;
	std::cout << "Server Version: " << Utility::serverVersion(session) << std::endl;
	std::cout << "Host Info: " << Utility::hostInfo(session) << std::endl;
}


void MySQLTest::connectNoDB()
{
	std::string dbConnString = "host=" MYSQL_HOST
		";user=" MYSQL_USER
		";password=" MYSQL_PWD
		";compress=true;auto-reconnect=true";

	try
	{
		Session session(MySQL::Connector::KEY, dbConnString);
		std::cout << "Connected to [" << "MySQL" << "] without database." << std::endl;
		dbInfo(session);
		session << "CREATE DATABASE IF NOT EXISTS " MYSQL_DB ";", now;
		std::cout << "Disconnecting ..." << std::endl;
		session.close();
		std::cout << "Disconnected." << std::endl;
	}
	catch (ConnectionFailedException& ex)
	{
		std::cout << ex.displayText() << std::endl;
	}
}


void MySQLTest::testBareboneMySQL()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third VARBINARY(30),"
		"Fourth INTEGER,"
		"Fifth FLOAT)";

	_pExecutor->bareboneMySQLTest(MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_DB, MYSQL_PORT, tableCreateString.c_str());
}


void MySQLTest::testSimpleAccess()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->simpleAccess();
}


void MySQLTest::testComplexType()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->complexType();
}


void MySQLTest::testSimpleAccessVector()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->simpleAccessVector();
}


void MySQLTest::testComplexTypeVector()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->complexTypeVector();
}


void MySQLTest::testInsertVector()
{
	if (!_pSession) fail ("Test not available.");

	recreateStringsTable();
	_pExecutor->insertVector();
}


void MySQLTest::testInsertEmptyVector()
{
	if (!_pSession) fail ("Test not available.");

	recreateStringsTable();
	_pExecutor->insertEmptyVector();
}


void MySQLTest::testInsertSingleBulk()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->insertSingleBulk();
}


void MySQLTest::testInsertSingleBulkVec()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->insertSingleBulkVec();
}


void MySQLTest::testLimit()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->limits();
}


void MySQLTest::testLimitZero()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->limitZero();
}


void MySQLTest::testLimitOnce()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->limitOnce();
}


void MySQLTest::testLimitPrepare()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->limitPrepare();
}



void MySQLTest::testPrepare()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->prepare();
}


void MySQLTest::testSetSimple()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->setSimple();
}


void MySQLTest::testSetComplex()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->setComplex();
}


void MySQLTest::testSetComplexUnique()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->setComplexUnique();
}

void MySQLTest::testMultiSetSimple()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->multiSetSimple();
}


void MySQLTest::testMultiSetComplex()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->multiSetComplex();
}


void MySQLTest::testMapComplex()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->mapComplex();
}


void MySQLTest::testMapComplexUnique()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->mapComplexUnique();
}


void MySQLTest::testMultiMapComplex()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->multiMapComplex();
}


void MySQLTest::testSelectIntoSingle()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->selectIntoSingle();
}


void MySQLTest::testSelectIntoSingleStep()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->selectIntoSingleStep();
}


void MySQLTest::testSelectIntoSingleFail()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->selectIntoSingleFail();
}


void MySQLTest::testLowerLimitOk()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->lowerLimitOk();
}


void MySQLTest::testSingleSelect()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->singleSelect();
}


void MySQLTest::testLowerLimitFail()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->lowerLimitFail();
}


void MySQLTest::testCombinedLimits()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->combinedLimits();
}



void MySQLTest::testRange()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->ranges();
}


void MySQLTest::testCombinedIllegalLimits()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->combinedIllegalLimits();
}



void MySQLTest::testIllegalRange()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->illegalRange();
}


void MySQLTest::testEmptyDB()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->emptyDB();
}


void MySQLTest::testDateTime()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonDateTimeTable();
	_pExecutor->dateTime();
	recreatePersonDateTable();
	_pExecutor->date();
	recreatePersonTimeTable();
	_pExecutor->time();
}


void MySQLTest::testBLOB()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonBLOBTable();
	_pExecutor->blob();

	const std::size_t maxFldSize = 65534;
	_pSession->setProperty("maxFieldSize", Poco::Any(maxFldSize-1));
	recreatePersonBLOBTable();

	try
	{
		_pExecutor->blob(maxFldSize);
		fail ("must fail");
	}
	catch (DataException&)
	{
		_pSession->setProperty("maxFieldSize", Poco::Any(maxFldSize));
	}

	recreatePersonBLOBTable();
	_pExecutor->blob(maxFldSize);

	recreatePersonBLOBTable();

	try
	{
		_pExecutor->blob(maxFldSize+1);
		fail ("must fail");
	}
	catch (DataException&) { }
}


void MySQLTest::testBLOBStmt()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonBLOBTable();
	_pExecutor->blobStmt();
}


void MySQLTest::testUnsignedInts()
{
	if (!_pSession) fail ("Test not available.");

	recreateUnsignedIntsTable();
	_pExecutor->unsignedInts();
}


void MySQLTest::testFloat()
{
	if (!_pSession) fail ("Test not available.");

	recreateFloatsTable();
	_pExecutor->floats();
}


void MySQLTest::testDouble()
{
	if (!_pSession) fail ("Test not available.");

	recreateFloatsTable();
	_pExecutor->doubles();
}


void MySQLTest::testTuple()
{
	if (!_pSession) fail ("Test not available.");

	recreateTuplesTable();
	_pExecutor->tuples();
}


void MySQLTest::testTupleVector()
{
	if (!_pSession) fail ("Test not available.");

	recreateTuplesTable();
	_pExecutor->tupleVector();
}


void MySQLTest::testInternalExtraction()
{
	if (!_pSession) fail ("Test not available.");

	recreateVectorsTable();
	_pExecutor->internalExtraction();
}


void MySQLTest::testNull()
{
	if (!_pSession) fail ("Test not available.");

	recreateVectorsTable();
	_pExecutor->doNull();
}


void MySQLTest::testSessionTransaction()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->sessionTransaction(_dbConnString);
}


void MySQLTest::testTransaction()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->transaction(_dbConnString);
}


void MySQLTest::testReconnect()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pExecutor->reconnect();
}


void MySQLTest::testNullableInt()
{
	if (!_pSession) fail ("Test not available.");

	recreateNullableIntTable();

	Nullable<Int32> i1(1);
	Nullable<Int32> i2;

	int id = 1;
	*_pSession << "INSERT INTO NullableIntTest VALUES(?, ?)", use(id), use(i1), now;
	id = 2;
	*_pSession << "INSERT INTO NullableIntTest VALUES(?, ?)", use(id), use(i2), now;
	id = 3;
	i2 = 3;
	*_pSession << "INSERT INTO NullableIntTest VALUES(?, ?)", use(id), use(i2), now;

	int count = 0;
	*_pSession << "SELECT COUNT(*) FROM NullableIntTest", into(count), now;
	assert (count == 3);

	Nullable<Int32> ci1;
	Nullable<Int32> ci2;
	Nullable<Int32> ci3;
	id = 1;
	*_pSession << "SELECT Value FROM NullableIntTest WHERE Id = ?", into(ci1), use(id), now;
	assert (ci1 == i1);
	id = 2;
	*_pSession << "SELECT Value FROM NullableIntTest WHERE Id = ?", into(ci2), use(id), now;
	assert (ci2.isNull());
	assert (!(0 == ci2));
	assert (0 != ci2);
	assert (!(ci2 == 0));
	assert (ci2 != 0);
	ci2 = 10;
	assert (10 == ci2);
	assert (ci2 == 10);
	assert (!ci2.isNull());
	id = 3;
	*_pSession << "SELECT Value FROM NullableIntTest WHERE Id = ?", into(ci3), use(id), now;
	assert (!ci3.isNull());
	assert (ci3 == 3);
	assert (3 == ci3);
}


void MySQLTest::testNullableString()
{
	if (!_pSession) fail ("Test not available.");

	recreateNullableStringTable();

	Int32 id = 0;
	Nullable<std::string> address("Address");
	Nullable<Int32> age = 10;
	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(id), use(address), use(age), now;
	id++;
	address = null;
	age = null;
	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(id), use(address), use(age), now;

	Nullable<std::string> resAddress;
	Nullable<Int32> resAge;
	*_pSession << "SELECT Address, Age FROM NullableStringTest WHERE Id = ?", into(resAddress), into(resAge), use(id), now;
	assert(resAddress == address);
	assert(resAge == age);
	assert(resAddress.isNull());
	assert(null == resAddress);
	assert(resAddress == null);

	resAddress = std::string("Test");
	assert(!resAddress.isNull());
	assert(resAddress == std::string("Test"));
	assert(std::string("Test") == resAddress);
	assert(null != resAddress);
	assert(resAddress != null);
}


void MySQLTest::testTupleWithNullable()
{
	if (!_pSession) fail ("Test not available.");

	recreateNullableStringTable();

	typedef Poco::Tuple<Int32, Nullable<std::string>, Nullable<Int32> > Info;

	Info info(0, std::string("Address"), 10);
	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(info), now;

	info.set<0>(info.get<0>()++);
	info.set<1>(null);
	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(info), now;

	info.set<0>(info.get<0>()++);
	info.set<1>(std::string("Address!"));
	info.set<2>(null);
	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(info), now;

	std::vector<Info> infos;
	infos.push_back(Info(10, std::string("A"), 0));
	infos.push_back(Info(11, null, 12));
	infos.push_back(Info(12, std::string("B"), null));

	*_pSession << "INSERT INTO NullableStringTest VALUES(?, ?, ?)", use(infos), now;

	std::vector<Info> result;

	*_pSession << "SELECT Id, Address, Age FROM NullableStringTest", into(result), now;

	assert(result[0].get<1>() == std::string("Address"));
	assert(result[0].get<2>() == 10);

	assert(result[1].get<1>() == null);
	assert(result[1].get<2>() == 10);

	assert(result[2].get<1>() == std::string("Address!"));
	assert(result[2].get<2>() == null);

	assert(result[3].get<1>() == std::string("A"));
	assert(result[3].get<2>() == 0);

	assert(result[4].get<1>() == null);
	assert(result[4].get<2>() == 12);

	assert(result[5].get<1>() == std::string("B"));
	assert(result[5].get<2>() == null);

}


void MySQLTest::dropTable(const std::string& tableName)
{
	try { *_pSession << format("DROP TABLE IF EXISTS %s", tableName), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("dropTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("dropTable()"); }
}


void MySQLTest::recreatePersonTable()
{
	dropTable("Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreatePersonTable()"); }
}


void MySQLTest::recreatePersonBLOBTable()
{
	dropTable("Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BLOB)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreatePersonBLOBTable()"); }
}


void MySQLTest::recreatePersonDateTimeTable()
{
	dropTable("Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Birthday DATETIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void MySQLTest::recreatePersonDateTable()
{
	dropTable("Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Birthday DATE)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreatePersonDateTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreatePersonDateTable()"); }
}


void MySQLTest::recreatePersonTimeTable()
{
	dropTable("Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Birthday TIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreatePersonTimeTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreatePersonTimeTable()"); }
}


void MySQLTest::recreateIntsTable()
{
	dropTable("Strings");
	try { *_pSession << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateIntsTable()"); }
}


void MySQLTest::recreateStringsTable()
{
	dropTable("Strings");
	try { *_pSession << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateStringsTable()"); }
}


void MySQLTest::recreateUnsignedIntsTable()
{
	dropTable("Strings");
	try { *_pSession << "CREATE TABLE Strings (str INTEGER UNSIGNED)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateUnsignedIntegersTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateUnsignedIntegersTable()"); }
}


void MySQLTest::recreateFloatsTable()
{
	dropTable("Strings");
	try { *_pSession << "CREATE TABLE Strings (str FLOAT)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateFloatsTable()"); }
}


void MySQLTest::recreateTuplesTable()
{
	dropTable("Tuples");
	try { *_pSession << "CREATE TABLE Tuples "
		"(i0 INTEGER, i1 INTEGER, i2 INTEGER, i3 INTEGER, i4 INTEGER, i5 INTEGER, i6 INTEGER, "
		"i7 INTEGER, i8 INTEGER, i9 INTEGER, i10 INTEGER, i11 INTEGER, i12 INTEGER, i13 INTEGER,"
		"i14 INTEGER, i15 INTEGER, i16 INTEGER, i17 INTEGER, i18 INTEGER, i19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateTuplesTable()"); }
}


void MySQLTest::recreateNullableIntTable()
{
	dropTable("NullableIntTest");
	try {
		*_pSession << "CREATE TABLE NullableIntTest (Id INTEGER(10), Value INTEGER(10))", now;
	}
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateNullableIntTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateNullableIntTable()"); }
}


void MySQLTest::recreateNullableStringTable()
{
	dropTable("NullableStringTest");
	try {
		*_pSession << "CREATE TABLE NullableStringTest (Id INTEGER(10), Address VARCHAR(30), Age INTEGER(10))", now;
	}
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateNullableStringTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateNullableStringTable()"); }
}


void MySQLTest::recreateVectorsTable()
{
	dropTable("Vectors");
	try { *_pSession << "CREATE TABLE Vectors (i0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail ("recreateVectorsTable()"); }
}


void MySQLTest::setUp()
{
}


void MySQLTest::tearDown()
{
	dropTable("Person");
	dropTable("Strings");
}


CppUnit::Test* MySQLTest::suite()
{
	MySQL::Connector::registerConnector();

	try
	{
		_pSession = new Session(MySQL::Connector::KEY, _dbConnString);
	}
	catch (ConnectionFailedException& ex)
	{
		std::cout << ex.displayText() << std::endl;
		std::cout << "Trying to connect without DB and create one ..." << std::endl;
		connectNoDB();
		try
		{
			_pSession = new Session(MySQL::Connector::KEY, _dbConnString);
		}
		catch (ConnectionFailedException& ex)
		{
			std::cout << ex.displayText() << std::endl;
			return 0;
		}
	}

	std::cout << "*** Connected to [" << "MySQL" << "] test database." << std::endl;
	dbInfo(*_pSession);

	_pExecutor = new SQLExecutor("MySQL SQL Executor", _pSession);

	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MySQLTest");

	CppUnit_addTest(pSuite, MySQLTest, testBareboneMySQL);
	CppUnit_addTest(pSuite, MySQLTest, testSimpleAccess);
	CppUnit_addTest(pSuite, MySQLTest, testComplexType);
	CppUnit_addTest(pSuite, MySQLTest, testSimpleAccessVector);
	CppUnit_addTest(pSuite, MySQLTest, testComplexTypeVector);
	CppUnit_addTest(pSuite, MySQLTest, testInsertVector);
	CppUnit_addTest(pSuite, MySQLTest, testInsertEmptyVector);
	CppUnit_addTest(pSuite, MySQLTest, testInsertSingleBulk);
	CppUnit_addTest(pSuite, MySQLTest, testInsertSingleBulkVec);
	CppUnit_addTest(pSuite, MySQLTest, testLimit);
	CppUnit_addTest(pSuite, MySQLTest, testLimitOnce);
	CppUnit_addTest(pSuite, MySQLTest, testLimitPrepare);
	CppUnit_addTest(pSuite, MySQLTest, testLimitZero);
	CppUnit_addTest(pSuite, MySQLTest, testPrepare);
	CppUnit_addTest(pSuite, MySQLTest, testSetSimple);
	CppUnit_addTest(pSuite, MySQLTest, testSetComplex);
	CppUnit_addTest(pSuite, MySQLTest, testSetComplexUnique);
	CppUnit_addTest(pSuite, MySQLTest, testMultiSetSimple);
	CppUnit_addTest(pSuite, MySQLTest, testMultiSetComplex);
	CppUnit_addTest(pSuite, MySQLTest, testMapComplex);
	CppUnit_addTest(pSuite, MySQLTest, testMapComplexUnique);
	CppUnit_addTest(pSuite, MySQLTest, testMultiMapComplex);
	CppUnit_addTest(pSuite, MySQLTest, testSelectIntoSingle);
	CppUnit_addTest(pSuite, MySQLTest, testSelectIntoSingleStep);
	CppUnit_addTest(pSuite, MySQLTest, testSelectIntoSingleFail);
	CppUnit_addTest(pSuite, MySQLTest, testLowerLimitOk);
	CppUnit_addTest(pSuite, MySQLTest, testLowerLimitFail);
	CppUnit_addTest(pSuite, MySQLTest, testCombinedLimits);
	CppUnit_addTest(pSuite, MySQLTest, testCombinedIllegalLimits);
	CppUnit_addTest(pSuite, MySQLTest, testRange);
	CppUnit_addTest(pSuite, MySQLTest, testIllegalRange);
	CppUnit_addTest(pSuite, MySQLTest, testSingleSelect);
	CppUnit_addTest(pSuite, MySQLTest, testEmptyDB);
	CppUnit_addTest(pSuite, MySQLTest, testDateTime);
	//CppUnit_addTest(pSuite, MySQLTest, testBLOB);
	CppUnit_addTest(pSuite, MySQLTest, testBLOBStmt);
	CppUnit_addTest(pSuite, MySQLTest, testUnsignedInts);
	CppUnit_addTest(pSuite, MySQLTest, testFloat);
	CppUnit_addTest(pSuite, MySQLTest, testDouble);
	CppUnit_addTest(pSuite, MySQLTest, testTuple);
	CppUnit_addTest(pSuite, MySQLTest, testTupleVector);
	CppUnit_addTest(pSuite, MySQLTest, testInternalExtraction);
	CppUnit_addTest(pSuite, MySQLTest, testNull);
	CppUnit_addTest(pSuite, MySQLTest, testNullableInt);
	CppUnit_addTest(pSuite, MySQLTest, testNullableString);
	CppUnit_addTest(pSuite, MySQLTest, testTupleWithNullable);
	CppUnit_addTest(pSuite, MySQLTest, testSessionTransaction);
	CppUnit_addTest(pSuite, MySQLTest, testTransaction);
	CppUnit_addTest(pSuite, MySQLTest, testReconnect);

	return pSuite;
}
