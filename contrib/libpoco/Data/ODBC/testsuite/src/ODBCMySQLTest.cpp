//
// ODBCMySQLTest.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCMySQLTest.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCMySQLTest.h"
#include "ODBCTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Format.h"
#include "Poco/Tuple.h"
#include "Poco/Exception.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/ODBC/Connector.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Data/ODBC/ODBCStatementImpl.h"
#include <sqltypes.h>
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::Data::DataException;
using Poco::Data::ODBC::Utility;
using Poco::Data::ODBC::ConnectionException;
using Poco::Data::ODBC::StatementException;
using Poco::Data::ODBC::StatementDiagnostics;
using Poco::format;
using Poco::Tuple;
using Poco::NotFoundException;


#define MYSQL_ODBC_DRIVER "MySQL ODBC 5.2 Driver"
#define MYSQL_DSN "PocoDataMySQLTest"
#define MYSQL_SERVER POCO_ODBC_TEST_DATABASE_SERVER
#define MYSQL_DB "test"
#define MYSQL_UID "root"
#define MYSQL_PWD "poco"


ODBCTest::SessionPtr ODBCMySQLTest::_pSession;
ODBCTest::ExecPtr    ODBCMySQLTest::_pExecutor;
std::string          ODBCMySQLTest::_driver = MYSQL_ODBC_DRIVER;
std::string          ODBCMySQLTest::_dsn = MYSQL_DSN;
std::string          ODBCMySQLTest::_uid = MYSQL_UID;
std::string          ODBCMySQLTest::_pwd = MYSQL_PWD;
std::string          ODBCMySQLTest::_connectString = "DRIVER={" MYSQL_ODBC_DRIVER "};"
	"DATABASE=" MYSQL_DB ";"
	"SERVER=" MYSQL_SERVER ";"
	"UID=" MYSQL_UID ";"
	"PWD=" MYSQL_PWD ";";


ODBCMySQLTest::ODBCMySQLTest(const std::string& name): 
	ODBCTest(name, _pSession, _pExecutor, _dsn, _uid, _pwd, _connectString)
{
	_pExecutor->execute("SET @@global.sql_mode= '';"); // disable strict mode
}


ODBCMySQLTest::~ODBCMySQLTest()
{
}


void ODBCMySQLTest::testBareboneODBC()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third VARBINARY(30),"
		"Fourth INTEGER,"
		"Fifth FLOAT,"
		"Sixth DATETIME)";

	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);

/*
MySQL supports batch statements as of 3.51.18
(http://bugs.mysql.com/bug.php?id=7445)
has different SQL syntax for it and behaves differently
compared to other DBMS systems in regards to SQLMoreResults.
So, we skip this test.

	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second INTEGER,"
		"Third FLOAT)";

	std::string MULTI_INSERT = "INSERT INTO Test VALUES "
		"('1', 2, 3.5),"
		"('2', 3, 4.5),"
		"('3', 4, 5.5),"
		"('4', 5, 6.5),"
		"('5', 6, 7.5);";

	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL, MULTI_INSERT);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND, MULTI_INSERT);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL, MULTI_INSERT);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND, MULTI_INSERT);
*/
}


void ODBCMySQLTest::testBLOB()
{
	if (!_pSession) fail ("Test not available.");
	
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

	for (int i = 0; i < 8;)
	{
		recreatePersonBLOBTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->blob(maxFldSize);
		i += 2;
	}

	recreatePersonBLOBTable();
	try
	{
		_pExecutor->blob(maxFldSize+1);
		fail ("must fail");
	}
	catch (DataException&) { }
}


void ODBCMySQLTest::testNull()
{
	if (!_pSession) fail ("Test not available.");

	// test for NOT NULL violation exception
	for (int i = 0; i < 8;)
	{
		recreateNullsTable("NOT NULL");
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->notNulls("HYT00");
		i += 2;
	}

	// test for null insertion
	for (int i = 0; i < 8;)
	{
		recreateNullsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->nulls();
		i += 2;
	}
}


void ODBCMySQLTest::testStoredProcedure()
{
	//MySQL is currently buggy in this area: 
	// http://bugs.mysql.com/bug.php?id=17898
	// http://bugs.mysql.com/bug.php?id=27632
	// Additionally, the standard ODBC stored procedure call syntax 
	// {call storedProcedure(?)} is currently (3.51.12.00) not supported.
	// See http://bugs.mysql.com/bug.php?id=26535
	// Poco::Data support for MySQL ODBC is postponed until the above
	// issues are resolved.
}


void ODBCMySQLTest::testStoredFunction()
{
	//MySQL is currently buggy in this area: 
	// http://bugs.mysql.com/bug.php?id=17898
	// http://bugs.mysql.com/bug.php?id=27632
	// Additionally, the standard ODBC stored procedure call syntax 
	// {call storedProcedure(?)} is currently (3.51.12.00) not supported.
	// See http://bugs.mysql.com/bug.php?id=26535
	// Poco::Data support for MySQL ODBC is postponed until the above
	// issues are resolved.
}


void ODBCMySQLTest::testMultipleResults()
{
/*
MySQL supports batch statements as of 3.51.18
http://bugs.mysql.com/bug.php?id=7445

	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->multipleResults();

		i += 2;
	}
*/
}


void ODBCMySQLTest::testFilter()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateVectorsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->filter("SELECT * FROM Vectors ORDER BY i0 ASC", "i0");
		i += 2;
	}
}


void ODBCMySQLTest::dropObject(const std::string& type, const std::string& name)
{
	*_pSession << format("DROP %s IF EXISTS %s", type, name), now;
}


void ODBCMySQLTest::recreateNullableTable()
{
	dropObject("TABLE", "NullableTest");
	try { *_pSession << "CREATE TABLE NullableTest (EmptyString VARCHAR(30) NULL, EmptyInteger INTEGER NULL, EmptyFloat FLOAT NULL , EmptyDateTime TIMESTAMP NULL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCMySQLTest::recreatePersonTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCMySQLTest::recreatePersonBLOBTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BLOB)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
}


void ODBCMySQLTest::recreatePersonDateTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornDate DATE)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTable()"); }
}


void ODBCMySQLTest::recreatePersonTimeTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornTime TIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
}


void ODBCMySQLTest::recreatePersonDateTimeTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Born DATETIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void ODBCMySQLTest::recreateIntsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateIntsTable()"); }
}


void ODBCMySQLTest::recreateStringsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateStringsTable()"); }
}


void ODBCMySQLTest::recreateFloatsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str FLOAT)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateFloatsTable()"); }
}


void ODBCMySQLTest::recreateTuplesTable()
{
	dropObject("TABLE", "Tuples");
	try { *_pSession << "CREATE TABLE Tuples "
		"(i0 INTEGER, i1 INTEGER, i2 INTEGER, i3 INTEGER, i4 INTEGER, i5 INTEGER, i6 INTEGER, "
		"i7 INTEGER, i8 INTEGER, i9 INTEGER, i10 INTEGER, i11 INTEGER, i12 INTEGER, i13 INTEGER,"
		"i14 INTEGER, i15 INTEGER, i16 INTEGER, i17 INTEGER, i18 INTEGER, i19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateTuplesTable()"); }
}


void ODBCMySQLTest::recreateVectorsTable()
{
	dropObject("TABLE", "Vectors");
	try { *_pSession << "CREATE TABLE Vectors (i0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateVectorsTable()"); }
}


void ODBCMySQLTest::recreateAnysTable()
{
	dropObject("TABLE", "Anys");
	try { *_pSession << "CREATE TABLE Anys (i0 INTEGER, flt0 DOUBLE, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateAnysTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateAnysTable()"); }
}


void ODBCMySQLTest::recreateNullsTable(const std::string& notNull)
{
	dropObject("TABLE", "NullTest");
	try { *_pSession << format("CREATE TABLE NullTest (i INTEGER %s, r FLOAT %s, v VARCHAR(30) %s)",
		notNull,
		notNull,
		notNull), now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCMySQLTest::recreateMiscTable()
{
	dropObject("TABLE", "MiscTest");
	try { *_pSession << "CREATE TABLE MiscTest "
		"(First VARCHAR(30),"
		"Second VARBINARY(30),"
		"Third INTEGER,"
		"Fourth FLOAT,"
		"Fifth DATETIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCMySQLTest::recreateLogTable()
{
	dropObject("TABLE", "T_POCO_LOG");
	dropObject("TABLE", "T_POCO_LOG_ARCHIVE");

	try 
	{ 
		std::string sql = "CREATE TABLE %s "
			"(Source VARCHAR(100),"
			"Name VARCHAR(100),"
			"ProcessId INTEGER,"
			"Thread VARCHAR(100), "
			"ThreadId INTEGER," 
			"Priority INTEGER,"
			"Text VARCHAR(100),"
			"DateTime DATETIME)"; 

		session() << sql, "T_POCO_LOG", now; 
		session() << sql, "T_POCO_LOG_ARCHIVE", now;

	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateLogTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateLogTable()"); }
}


CppUnit::Test* ODBCMySQLTest::suite()
{
	if ((_pSession = init(_driver, _dsn, _uid, _pwd, _connectString)))
	{
		std::cout << "*** Connected to [" << _driver << "] test database." << std::endl;

		_pExecutor = new SQLExecutor(_driver + " SQL Executor", _pSession);

		CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCMySQLTest");

		CppUnit_addTest(pSuite, ODBCMySQLTest, testBareboneODBC);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testZeroRows);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSimpleAccess);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testComplexType);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSimpleAccessVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSharedPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testAutoPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertEmptyVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSimpleAccessList);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testComplexTypeList);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertList);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertEmptyList);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSimpleAccessDeque);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testComplexTypeDeque);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertDeque);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertEmptyDeque);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testAffectedRows);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertSingleBulk);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInsertSingleBulkVec);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLimit);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLimitOnce);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLimitPrepare);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLimitZero);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testPrepare);
		//CppUnit_addTest(pSuite, ODBCMySQLTest, testBulk);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testBulkPerformance);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSetSimple);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSetComplex);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSetComplexUnique);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testMultiSetSimple);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testMultiSetComplex);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testMapComplex);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testMapComplexUnique);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testMultiMapComplex);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSelectIntoSingle);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSelectIntoSingleStep);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSelectIntoSingleFail);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLowerLimitOk);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testLowerLimitFail);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testCombinedLimits);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testCombinedIllegalLimits);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testRange);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testIllegalRange);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSingleSelect);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testEmptyDB);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testBLOB);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testBLOBContainer);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testBLOBStmt);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testDate);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testTime);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testDateTime);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testFloat);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testDouble);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testTuple);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testTupleVector);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testStoredProcedure);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testStoredFunction);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInternalExtraction);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testFilter);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInternalBulkExtraction);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testInternalStorageType);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testNull);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testRowIterator);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testAsync);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testAny);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testDynamicAny);
		//CppUnit_addTest(pSuite, ODBCMySQLTest, testMultipleResults);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSQLChannel);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSQLLogger);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testSessionTransaction);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testTransaction);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testTransactor);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testNullable);
		CppUnit_addTest(pSuite, ODBCMySQLTest, testReconnect);

		return pSuite;
	}

	return 0;
}
