//
// ODBCDB2Test.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCDB2Test.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCDB2Test.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Format.h"
#include "Poco/Any.h"
#include "Poco/DynamicAny.h"
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
using Poco::Any;
using Poco::AnyCast;
using Poco::DynamicAny;
using Poco::NotFoundException;


#define DB2_ODBC_DRIVER "IBM DB2 ODBC DRIVER - DB2COPY1"
#define DB2_DSN "PocoDataDB2Test"
#define DB2_SERVER POCO_ODBC_TEST_DATABASE_SERVER
#define DB2_PORT "50000"
#define DB2_DB "POCOTEST"
#define DB2_UID "db2admin"
#define DB2_PWD "db2admin"


ODBCTest::SessionPtr ODBCDB2Test::_pSession;
ODBCTest::ExecPtr    ODBCDB2Test::_pExecutor;
std::string          ODBCDB2Test::_driver = DB2_ODBC_DRIVER;
std::string          ODBCDB2Test::_dsn = DB2_DSN;
std::string          ODBCDB2Test::_uid = DB2_UID;
std::string          ODBCDB2Test::_pwd = DB2_PWD;
std::string          ODBCDB2Test::_connectString = "Driver=" DB2_ODBC_DRIVER ";"
	"Database=" DB2_DB ";"
	"Hostname=" DB2_SERVER ";"
	"Port=" DB2_PORT ";"
	"Protocol=TCPIP;"
	"Uid=" DB2_UID ";"
	"Pwd=" DB2_PWD ";";


ODBCDB2Test::ODBCDB2Test(const std::string& name): 
	ODBCTest(name, _pSession, _pExecutor, _dsn, _uid, _pwd, _connectString)
{
}


ODBCDB2Test::~ODBCDB2Test()
{
}


void ODBCDB2Test::testBareboneODBC()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BLOB,"
		"Fourth INTEGER,"
		"Fifth FLOAT,"
		"Sixth TIMESTAMP)";

	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);


	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second INTEGER,"
		"Third FLOAT)";

	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCMultiResultTest(dbConnString(), tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);
}


void ODBCDB2Test::testBLOB()
{
	if (!_pSession) fail ("Test not available.");
	
	const std::size_t maxFldSize = 1000000;
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


void ODBCDB2Test::testFilter()
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


void ODBCDB2Test::testStoredProcedure()
{
	if (!_pSession) fail ("Test not available.");

	for (int k = 0; k < 8;)
	{
		_pSession->setFeature("autoBind", bindValue(k));
		_pSession->setFeature("autoExtract", bindValue(k+1));

		dropObject("PROCEDURE", "storedProcedure");
		*_pSession << "CREATE PROCEDURE storedProcedure(OUT outParam INTEGER) "
			"BEGIN "
			" SET outParam = -1; "
			"END" , now;

		int i = 0;
		*_pSession << "{call storedProcedure(?)}", out(i), now;
		assert(-1 == i);
		dropObject("PROCEDURE", "storedProcedure");

		*_pSession << "CREATE PROCEDURE storedProcedure(inParam INTEGER, OUT outParam INTEGER) "
			"BEGIN "
			" SET outParam = inParam*inParam; "
			"END" , now;
		

		i = 2;
		int j = 0;
		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == j);
		dropObject("PROCEDURE", "storedProcedure");
	
		*_pSession << "CREATE PROCEDURE storedProcedure(INOUT ioParam INTEGER) "
			"BEGIN "
			" SET ioParam = ioParam*ioParam; "
			"END" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == i);
		dropObject("PROCEDURE", "storedProcedure");

		//TIMESTAMP is not supported as stored procedure parameter in DB2
		//(SQL0182N An expression with a datetime value or a labeled duration is not valid.)

		*_pSession << "CREATE PROCEDURE storedProcedure(inParam VARCHAR(1000), OUT outParam VARCHAR(1000)) "
			"BEGIN "
			" SET outParam = inParam; "
			"END" , now;

		std::string inParam = 
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
			"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
		std::string outParam;
		*_pSession << "{call storedProcedure(?,?)}", in(inParam), out(outParam), now;
		assert(inParam == outParam);
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}
}


void ODBCDB2Test::testStoredProcedureAny()
{
	if (!_pSession) fail ("Test not available.");

	for (int k = 0; k < 8;)
	{
		_pSession->setFeature("autoBind", bindValue(k));
		_pSession->setFeature("autoExtract", bindValue(k+1));

		Any i = 2;
		Any j = 0;

		*_pSession << "CREATE PROCEDURE storedProcedure(inParam INTEGER, OUT outParam INTEGER) "
			"BEGIN "
			" SET outParam = inParam*inParam; "
			"END" , now;

		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == AnyCast<int>(j));
		*_pSession << "DROP PROCEDURE storedProcedure;", now;

		*_pSession << "CREATE PROCEDURE storedProcedure(INOUT ioParam INTEGER) "
			"BEGIN "
			" SET ioParam = ioParam*ioParam; "
			"END" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == AnyCast<int>(i));
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}
}


void ODBCDB2Test::testStoredProcedureDynamicAny()
{
	if (!_pSession) fail ("Test not available.");

	for (int k = 0; k < 8;)
	{
		_pSession->setFeature("autoBind", bindValue(k));
		
		DynamicAny i = 2;
		DynamicAny j = 0;

		*_pSession << "CREATE PROCEDURE storedProcedure(inParam INTEGER, OUT outParam INTEGER) "
			"BEGIN "
			" SET outParam = inParam*inParam; "
			"END" , now;

		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == j);
		*_pSession << "DROP PROCEDURE storedProcedure;", now;

		*_pSession << "CREATE PROCEDURE storedProcedure(INOUT ioParam INTEGER) "
			"BEGIN "
			" SET ioParam = ioParam*ioParam; "
			"END" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == i);
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}
}


void ODBCDB2Test::testStoredFunction()
{
	if (!_pSession) fail ("Test not available.");

	for (int k = 0; k < 8;)
	{
		_pSession->setFeature("autoBind", bindValue(k));
		_pSession->setFeature("autoExtract", bindValue(k+1));

		dropObject("PROCEDURE", "storedFunction");
		*_pSession << "CREATE PROCEDURE storedFunction() "
			"BEGIN "
			"  RETURN -1; "
			"END" , now;

		int i = 0;
		*_pSession << "{? = call storedFunction()}", out(i), now;
		assert(-1 == i);
		dropObject("PROCEDURE", "storedFunction");
		
		*_pSession << "CREATE PROCEDURE storedFunction(inParam INTEGER) "
			"BEGIN "
			" RETURN inParam*inParam; "
			"END" , now;
		
		i = 2;
		int result = 0;
		*_pSession << "{? = call storedFunction(?)}", out(result), in(i), now;
		assert(4 == result);
		dropObject("PROCEDURE", "storedFunction");

		*_pSession << "CREATE PROCEDURE storedFunction(inParam INTEGER, OUT outParam INTEGER) "
			"BEGIN "
			" SET outParam = inParam*inParam; "
			" RETURN outParam; "
			"END" , now;

		i = 2;
		int j = 0;
		result = 0;
		*_pSession << "{? = call storedFunction(?, ?)}", out(result), in(i), out(j), now;
		assert(4 == j);
		assert(j == result); 
		dropObject("PROCEDURE", "storedFunction");

		*_pSession << "CREATE PROCEDURE storedFunction(INOUT param1 INTEGER, INOUT param2 INTEGER) "
			"BEGIN "
			" DECLARE temp INTEGER;"
			" SET temp = param1; "
			" SET param1 = param2; "
			" SET param2 = temp; "
			" RETURN param1 + param2; "
			"END" , now;

		i = 1;
		j = 2;
		result = 0;
		*_pSession << "{? = call storedFunction(?, ?)}", out(result), io(i), io(j), now;
		assert(1 == j);
		assert(2 == i);
		assert(3 == result); 

		Tuple<int, int> params(1, 2);
		assert(1 == params.get<0>());
		assert(2 == params.get<1>());
		result = 0;
		*_pSession << "{? = call storedFunction(?, ?)}", out(result), io(params), now;
		assert(1 == params.get<1>());
		assert(2 == params.get<0>());
		assert(3 == result); 

		dropObject("PROCEDURE", "storedFunction");

		_pSession->setFeature("autoBind", true);

		*_pSession << "CREATE PROCEDURE storedFunction(inParam VARCHAR(10), OUT outParam VARCHAR(10)) "
			"BEGIN "
			" SET outParam = inParam; "
			" RETURN LENGTH(outParam);"//DB2 allows only integer as return type
			"END" , now;

		std::string inParam = "123456789";
		std::string outParam;
		int ret;
		*_pSession << "{? = call storedFunction(?,?)}", out(ret), in(inParam), out(outParam), now;
		assert(inParam == outParam);
		assert(ret == inParam.size());
		dropObject("PROCEDURE", "storedFunction");

		k += 2;
	}
}


void ODBCDB2Test::dropObject(const std::string& type, const std::string& name)
{
	try
	{
		*_pSession << format("DROP %s %s", type, name), now;
	}
	catch (StatementException& ex)
	{
		bool ignoreError = false;
		const StatementDiagnostics::FieldVec& flds = ex.diagnostics().fields();
		StatementDiagnostics::Iterator it = flds.begin();
		for (; it != flds.end(); ++it)
		{
			if (-204 == it->_nativeError)//(table does not exist)
			{
				ignoreError = true;
				break;
			}
		}

		if (!ignoreError) throw;
	}
}


void ODBCDB2Test::recreateNullableTable()
{
	dropObject("TABLE", "NullableTest");
	try { *_pSession << "CREATE TABLE NullableTest (EmptyString VARCHAR(30) NULL, EmptyInteger INTEGER NULL, EmptyFloat FLOAT NULL , EmptyDateTime TIMESTAMP NULL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCDB2Test::recreatePersonTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCDB2Test::recreatePersonBLOBTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BLOB)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
}


void ODBCDB2Test::recreatePersonDateTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornDate DATE)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTable()"); }
}


void ODBCDB2Test::recreatePersonTimeTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornTime TIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
}


void ODBCDB2Test::recreatePersonDateTimeTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Born TIMESTAMP)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void ODBCDB2Test::recreateIntsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateIntsTable()"); }
}


void ODBCDB2Test::recreateStringsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateStringsTable()"); }
}


void ODBCDB2Test::recreateFloatsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str FLOAT)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateFloatsTable()"); }
}


void ODBCDB2Test::recreateTuplesTable()
{
	dropObject("TABLE", "Tuples");
	try { *_pSession << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER, int10 INTEGER, int11 INTEGER, int12 INTEGER, int13 INTEGER,"
		"int14 INTEGER, int15 INTEGER, int16 INTEGER, int17 INTEGER, int18 INTEGER, int19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateTuplesTable()"); }
}


void ODBCDB2Test::recreateVectorsTable()
{
	dropObject("TABLE", "Vectors");
	try { *_pSession << "CREATE TABLE Vectors (i0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateVectorsTable()"); }
}


void ODBCDB2Test::recreateAnysTable()
{
	dropObject("TABLE", "Anys");
	try { *_pSession << "CREATE TABLE Anys (i0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateAnysTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateAnysTable()"); }
}


void ODBCDB2Test::recreateNullsTable(const std::string& notNull)
{
	dropObject("TABLE", "NullTest");
	try { *_pSession << format("CREATE TABLE NullTest (i INTEGER %s, r FLOAT %s, v VARCHAR(30) %s)",
		notNull,
		notNull,
		notNull), now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCDB2Test::recreateMiscTable()
{
	dropObject("TABLE", "MiscTest");
	try 
	{ 
		session() << "CREATE TABLE MiscTest "
			"(First VARCHAR(30),"
			"Second BLOB,"
			"Third INTEGER,"
			"Fourth FLOAT,"
			"Fifth TIMESTAMP)", now; 
	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateMiscTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateMiscTable()"); }
}


void ODBCDB2Test::recreateLogTable()
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
			"DateTime TIMESTAMP)"; 

		session() << sql, "T_POCO_LOG", now; 
		session() << sql, "T_POCO_LOG_ARCHIVE", now;

	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateLogTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateLogTable()"); }
}


CppUnit::Test* ODBCDB2Test::suite()
{
	if ((_pSession = init(_driver, _dsn, _uid, _pwd, _connectString)))
	{
		std::cout << "*** Connected to [" << _driver << "] test database." << std::endl;

		_pExecutor = new SQLExecutor(_driver + " SQL Executor", _pSession);

		CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCDB2Test");

		CppUnit_addTest(pSuite, ODBCDB2Test, testBareboneODBC);
		CppUnit_addTest(pSuite, ODBCDB2Test, testZeroRows);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSimpleAccess);
		CppUnit_addTest(pSuite, ODBCDB2Test, testComplexType);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSimpleAccessVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSharedPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testAutoPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertEmptyVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSimpleAccessList);
		CppUnit_addTest(pSuite, ODBCDB2Test, testComplexTypeList);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertList);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertEmptyList);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSimpleAccessDeque);
		CppUnit_addTest(pSuite, ODBCDB2Test, testComplexTypeDeque);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertDeque);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertEmptyDeque);
		CppUnit_addTest(pSuite, ODBCDB2Test, testAffectedRows);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertSingleBulk);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInsertSingleBulkVec);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLimit);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLimitOnce);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLimitPrepare);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLimitZero);
		CppUnit_addTest(pSuite, ODBCDB2Test, testPrepare);
		CppUnit_addTest(pSuite, ODBCDB2Test, testBulk);
		CppUnit_addTest(pSuite, ODBCDB2Test, testBulkPerformance);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSetSimple);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSetComplex);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSetComplexUnique);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMultiSetSimple);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMultiSetComplex);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMapComplex);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMapComplexUnique);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMultiMapComplex);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSelectIntoSingle);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSelectIntoSingleStep);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSelectIntoSingleFail);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLowerLimitOk);
		CppUnit_addTest(pSuite, ODBCDB2Test, testLowerLimitFail);
		CppUnit_addTest(pSuite, ODBCDB2Test, testCombinedLimits);
		CppUnit_addTest(pSuite, ODBCDB2Test, testCombinedIllegalLimits);
		CppUnit_addTest(pSuite, ODBCDB2Test, testRange);
		CppUnit_addTest(pSuite, ODBCDB2Test, testIllegalRange);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSingleSelect);
		CppUnit_addTest(pSuite, ODBCDB2Test, testEmptyDB);
		CppUnit_addTest(pSuite, ODBCDB2Test, testBLOB);
		CppUnit_addTest(pSuite, ODBCDB2Test, testBLOBContainer);
		CppUnit_addTest(pSuite, ODBCDB2Test, testBLOBStmt);
		CppUnit_addTest(pSuite, ODBCDB2Test, testDate);
		CppUnit_addTest(pSuite, ODBCDB2Test, testTime);
		CppUnit_addTest(pSuite, ODBCDB2Test, testDateTime);
		CppUnit_addTest(pSuite, ODBCDB2Test, testFloat);
		CppUnit_addTest(pSuite, ODBCDB2Test, testDouble);
		CppUnit_addTest(pSuite, ODBCDB2Test, testTuple);
		CppUnit_addTest(pSuite, ODBCDB2Test, testTupleVector);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInternalExtraction);
		CppUnit_addTest(pSuite, ODBCDB2Test, testFilter);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInternalBulkExtraction);
		CppUnit_addTest(pSuite, ODBCDB2Test, testInternalStorageType);
		CppUnit_addTest(pSuite, ODBCDB2Test, testStoredProcedure);
		CppUnit_addTest(pSuite, ODBCDB2Test, testStoredProcedureAny);
		CppUnit_addTest(pSuite, ODBCDB2Test, testStoredProcedureDynamicAny);
		CppUnit_addTest(pSuite, ODBCDB2Test, testStoredFunction);
		CppUnit_addTest(pSuite, ODBCDB2Test, testNull);
		CppUnit_addTest(pSuite, ODBCDB2Test, testRowIterator);
		CppUnit_addTest(pSuite, ODBCDB2Test, testAsync);
		CppUnit_addTest(pSuite, ODBCDB2Test, testAny);
		CppUnit_addTest(pSuite, ODBCDB2Test, testDynamicAny);
		CppUnit_addTest(pSuite, ODBCDB2Test, testMultipleResults);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSQLChannel);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSQLLogger);
		CppUnit_addTest(pSuite, ODBCDB2Test, testSessionTransaction);
		CppUnit_addTest(pSuite, ODBCDB2Test, testTransaction);
		CppUnit_addTest(pSuite, ODBCDB2Test, testTransactor);
		CppUnit_addTest(pSuite, ODBCDB2Test, testNullable);
		CppUnit_addTest(pSuite, ODBCDB2Test, testReconnect);

		return pSuite;
	}

	return 0;
}
