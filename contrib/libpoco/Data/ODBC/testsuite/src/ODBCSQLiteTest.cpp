//
// ODBCSQLiteTest.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCSQLiteTest.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCSQLiteTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Format.h"
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
using Poco::Data::ODBC::Utility;
using Poco::Data::ODBC::ConnectionException;
using Poco::Data::ODBC::StatementException;
using Poco::Data::ODBC::StatementDiagnostics;
using Poco::format;
using Poco::NotFoundException;


#define SQLITE_ODBC_DRIVER "SQLite3 ODBC Driver"
#define SQLITE_DSN "PocoDataSQLiteTest"
#define SQLITE_DB "dummy.db"


ODBCTest::SessionPtr ODBCSQLiteTest::_pSession;
ODBCTest::ExecPtr    ODBCSQLiteTest::_pExecutor;
std::string          ODBCSQLiteTest::_driver = SQLITE_ODBC_DRIVER;
std::string          ODBCSQLiteTest::_dsn = SQLITE_DSN;
std::string          ODBCSQLiteTest::_uid = "";
std::string          ODBCSQLiteTest::_pwd = "";
std::string          ODBCSQLiteTest::_connectString = "Driver=" SQLITE_ODBC_DRIVER 
	";Database=" SQLITE_DB ";";


ODBCSQLiteTest::ODBCSQLiteTest(const std::string& name): 
	ODBCTest(name, _pSession, _pExecutor, _dsn, _uid, _pwd, _connectString)
{
}


ODBCSQLiteTest::~ODBCSQLiteTest()
{
}


void ODBCSQLiteTest::testBareboneODBC()
{
	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BLOB,"
		"Fourth INTEGER,"
		"Fifth REAL,"
		"Sixth TIMESTAMP)";

	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);

	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BLOB,"
		"Fourth INTEGER,"
		"Fifth REAL,"
		"Sixth DATETIME)";

	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);

	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BLOB,"
		"Fourth INTEGER,"
		"Fifth REAL,"
		"Sixth DATE)";

	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);
}


void ODBCSQLiteTest::testAffectedRows()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		// see SQLiteStatementImpl::affectedRows() documentation for explanation
		// why "WHERE 1" is necessary here
		_pExecutor->affectedRows("WHERE 1");
		i += 2;
	}	
}


void ODBCSQLiteTest::testNull()
{
	if (!_pSession) fail ("Test not available.");

	// test for NOT NULL violation exception
	for (int i = 0; i < 8;)
	{
		recreateNullsTable("NOT NULL");
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		_pExecutor->notNulls("HY000");
		i += 2;
	}
}


void ODBCSQLiteTest::dropObject(const std::string& type, const std::string& name)
{
	try
	{
		session() << format("DROP %s %s", type, name), now;
	}
	catch (StatementException& ex)
	{
		bool ignoreError = false;
		const StatementDiagnostics::FieldVec& flds = ex.diagnostics().fields();
		StatementDiagnostics::Iterator it = flds.begin();
		for (; it != flds.end(); ++it)
		{
			if (1 == it->_nativeError)//(no such table)
			{
				ignoreError = true;
				break;
			}
		}

		if (!ignoreError) 
		{
			std::cout << ex.toString() << std::endl;
			throw;
		}
	}
}


void ODBCSQLiteTest::recreateNullableTable()
{
	dropObject("TABLE", "NullableTest");
	try { *_pSession << "CREATE TABLE NullableTest (EmptyString VARCHAR(30) NULL, EmptyInteger INTEGER NULL, EmptyFloat REAL NULL , EmptyDateTime TIMESTAMP NULL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCSQLiteTest::recreatePersonTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCSQLiteTest::recreatePersonBLOBTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BLOB)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
}


void ODBCSQLiteTest::recreatePersonDateTimeTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Born TIMESTAMP)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void ODBCSQLiteTest::recreateIntsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateIntsTable()"); }
}


void ODBCSQLiteTest::recreateStringsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateStringsTable()"); }
}


void ODBCSQLiteTest::recreateFloatsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str REAL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateFloatsTable()"); }
}


void ODBCSQLiteTest::recreateTuplesTable()
{
	dropObject("TABLE", "Tuples");
	try { session() << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER, int10 INTEGER, int11 INTEGER, int12 INTEGER, int13 INTEGER,"
		"int14 INTEGER, int15 INTEGER, int16 INTEGER, int17 INTEGER, int18 INTEGER, int19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateTuplesTable()"); }
}


void ODBCSQLiteTest::recreateVectorsTable()
{
	dropObject("TABLE", "Vectors");
	try { session() << "CREATE TABLE Vectors (int0 INTEGER, flt0 REAL, str0 VARCHAR)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateVectorsTable()"); }
}


void ODBCSQLiteTest::recreateAnysTable()
{
	dropObject("TABLE", "Anys");
	try { session() << "CREATE TABLE Anys (int0 INTEGER, flt0 REAL, str0 VARCHAR)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateAnysTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateAnysTable()"); }
}


void ODBCSQLiteTest::recreateNullsTable(const std::string& notNull)
{
	dropObject("TABLE", "NullTest");
	try { session() << format("CREATE TABLE NullTest (i INTEGER %s, r REAL %s, v VARCHAR(30) %s)",
		notNull,
		notNull,
		notNull), now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCSQLiteTest::recreateMiscTable()
{
	dropObject("TABLE", "MiscTest");
	try 
	{ 
		// SQLite fails with BLOB bulk operations
		session() << "CREATE TABLE MiscTest "
			"(First VARCHAR(30),"
			//"Second BLOB,"
			"Third INTEGER,"
			"Fourth REAL,"
			"Fifth DATETIME)", now; 
	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateMiscTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateMiscTable()"); }
}


void ODBCSQLiteTest::recreateLogTable()
{
	dropObject("TABLE", "T_POCO_LOG");
	dropObject("TABLE", "T_POCO_LOG_ARCHIVE");

	try 
	{ 
		std::string sql = "CREATE TABLE %s "
			"(Source VARCHAR,"
			"Name VARCHAR,"
			"ProcessId INTEGER,"
			"Thread VARCHAR, "
			"ThreadId INTEGER," 
			"Priority INTEGER,"
			"Text VARCHAR,"
			"DateTime DATETIME)";

		session() << sql, "T_POCO_LOG", now; 
		session() << sql, "T_POCO_LOG_ARCHIVE", now; 

	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateLogTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateLogTable()"); }
}


CppUnit::Test* ODBCSQLiteTest::suite()
{
	if ((_pSession = init(_driver, _dsn, _uid, _pwd, _connectString)))
	{
		std::cout << "*** Connected to [" << _driver << "] test database." << std::endl;

		_pExecutor = new SQLExecutor(_driver + " SQL Executor", _pSession);

		CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCSQLiteTest");

		CppUnit_addTest(pSuite, ODBCSQLiteTest, testBareboneODBC);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testZeroRows);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSimpleAccess);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testComplexType);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSimpleAccessVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSharedPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testAutoPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertEmptyVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSimpleAccessList);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testComplexTypeList);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertList);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertEmptyList);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSimpleAccessDeque);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testComplexTypeDeque);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertDeque);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertEmptyDeque);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testAffectedRows);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertSingleBulk);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInsertSingleBulkVec);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLimit);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLimitOnce);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLimitPrepare);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLimitZero);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testPrepare);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSetSimple);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSetComplex);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSetComplexUnique);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testMultiSetSimple);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testMultiSetComplex);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testMapComplex);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testMapComplexUnique);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testMultiMapComplex);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSelectIntoSingle);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSelectIntoSingleStep);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSelectIntoSingleFail);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLowerLimitOk);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testLowerLimitFail);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testCombinedLimits);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testCombinedIllegalLimits);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testRange);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testIllegalRange);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSingleSelect);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testEmptyDB);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testBLOB);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testBLOBContainer);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testBLOBStmt);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testDateTime);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testFloat);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testDouble);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testTuple);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testTupleVector);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInternalExtraction);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testFilter);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testInternalStorageType);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testNull);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testRowIterator);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testAsync);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testAny);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testDynamicAny);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSQLChannel);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSQLLogger);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testSessionTransaction);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testTransaction);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testTransactor);
		CppUnit_addTest(pSuite, ODBCSQLiteTest, testReconnect);

		return pSuite;
	}

	return 0;
}
