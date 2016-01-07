	//
// ODBCPostgreSQLTest.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCPostgreSQLTest.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCPostgreSQLTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "ODBCTest.h"
#include "Poco/Format.h"
#include "Poco/Any.h"
#include "Poco/DynamicAny.h"
#include "Poco/DateTime.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::Data::DataException;
using Poco::Data::ODBC::ConnectionException;
using Poco::Data::ODBC::StatementException;
using Poco::Data::ODBC::StatementDiagnostics;
using Poco::format;
using Poco::Any;
using Poco::AnyCast;
using Poco::DynamicAny;
using Poco::DateTime;


#ifdef POCO_ODBC_USE_MAMMOTH_NG
	#define POSTGRESQL_ODBC_DRIVER "Mammoth ODBCng Beta"
#elif defined (POCO_ODBC_UNICODE)
	#ifdef POCO_PTR_IS_64_BIT
		#define POSTGRESQL_ODBC_DRIVER "PostgreSQL Unicode(x64)"
	#else
		#define POSTGRESQL_ODBC_DRIVER "PostgreSQL Unicode"
	#endif
	#define POSTGRESQL_DSN "PocoDataPgSQLTestW"
#else
	#ifdef POCO_PTR_IS_64_BIT
		#define POSTGRESQL_ODBC_DRIVER "PostgreSQL ANSI(x64)"
	#else
		#define POSTGRESQL_ODBC_DRIVER "PostgreSQL ANSI"
	#endif
	#define POSTGRESQL_DSN "PocoDataPgSQLTest"
#endif

#if defined(POCO_OS_FAMILY_WINDOWS)
	#pragma message ("Using " POSTGRESQL_ODBC_DRIVER " driver.")
#endif

#define POSTGRESQL_SERVER POCO_ODBC_TEST_DATABASE_SERVER
#define POSTGRESQL_PORT    "5432"
#define POSTGRESQL_DB      "postgres"
#define POSTGRESQL_UID     "postgres"
#define POSTGRESQL_PWD     "postgres"
#define POSTGRESQL_VERSION "9.3"

#ifdef POCO_OS_FAMILY_WINDOWS
const std::string ODBCPostgreSQLTest::_libDir = "C:\\\\Program Files\\\\PostgreSQL\\\\" POSTGRESQL_VERSION "\\\\lib\\\\";
#else
const std::string ODBCPostgreSQLTest::_libDir = "/usr/local/pgsql/lib/";
#endif


ODBCTest::SessionPtr ODBCPostgreSQLTest::_pSession;
ODBCTest::ExecPtr    ODBCPostgreSQLTest::_pExecutor;
std::string          ODBCPostgreSQLTest::_driver = POSTGRESQL_ODBC_DRIVER;
std::string          ODBCPostgreSQLTest::_dsn = POSTGRESQL_DSN;
std::string          ODBCPostgreSQLTest::_uid = POSTGRESQL_UID;
std::string          ODBCPostgreSQLTest::_pwd = POSTGRESQL_PWD;
std::string ODBCPostgreSQLTest::_connectString = 
	"DRIVER=" POSTGRESQL_ODBC_DRIVER ";"
	"DATABASE=" POSTGRESQL_DB ";"
	"SERVER=" POSTGRESQL_SERVER ";"
	"PORT=" POSTGRESQL_PORT ";"
	"UID=" POSTGRESQL_UID ";"
	"PWD=" POSTGRESQL_PWD ";"
	"SSLMODE=prefer;"
	"LowerCaseIdentifier=0;"
	"UseServerSidePrepare=0;"
	"ByteaAsLongVarBinary=1;"
	"BI=0;"
	"TrueIsMinus1=0;"
	"DisallowPremature=0;"
	"UpdatableCursors=0;"
	"LFConversion=1;"
	"CancelAsFreeStmt=0;"
	"Parse=0;"
	"BoolsAsChar=1;"
	"UnknownsAsLongVarchar=0;"
	"TextAsLongVarchar=1;"
	"UseDeclareFetch=0;"
	"Ksqo=1;"
	"Optimizer=1;"
	"CommLog=0;"
	"Debug=0;"
	"MaxLongVarcharSize=8190;"
	"MaxVarcharSize=254;"
	"UnknownSizes=0;"
	"Socket=8192;"
	"Fetch=100;"
	"ConnSettings=;"
	"ShowSystemTables=0;"
	"RowVersioning=0;"
	"ShowOidColumn=0;"
	"FakeOidIndex=0;"
	"ReadOnly=0;";


ODBCPostgreSQLTest::ODBCPostgreSQLTest(const std::string& name): 
	ODBCTest(name, _pSession, _pExecutor, _dsn, _uid, _pwd, _connectString)
{
}


ODBCPostgreSQLTest::~ODBCPostgreSQLTest()
{
}


void ODBCPostgreSQLTest::testBareboneODBC()
{
	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BYTEA,"
		"Fourth INTEGER,"
		"Fifth FLOAT,"
		"Sixth TIMESTAMP)";

	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);

	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BYTEA,"
		"Fourth INTEGER,"
		"Fifth FLOAT,"
		"Sixth DATE)";

	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL, false);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND, false);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL, false);
	executor().bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND, false);

//neither pSQL ODBC nor Mammoth drivers support multiple results properly
/*
	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second INTEGER,"
		"Third FLOAT)";

	executor().bareboneODBCMultiResultTest(_dbConnString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCMultiResultTest(_dbConnString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	executor().bareboneODBCMultiResultTest(_dbConnString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	executor().bareboneODBCMultiResultTest(_dbConnString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);
*/
}


void ODBCPostgreSQLTest::testBLOB()
{
	const std::size_t maxFldSize = 1000000;
	session().setProperty("maxFieldSize", Poco::Any(maxFldSize-1));
	recreatePersonBLOBTable();

	try
	{
		executor().blob(maxFldSize);
		fail ("must fail");
	}
	catch (DataException&) 
	{
		session().setProperty("maxFieldSize", Poco::Any(maxFldSize));
	}

	for (int i = 0; i < 8;)
	{
		recreatePersonBLOBTable();
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		executor().blob(1000000);
		i += 2;
	}
}


void ODBCPostgreSQLTest::testStoredFunction()
{
	configurePLPgSQL();

	std::string func("testStoredFunction()");

	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		dropObject("FUNCTION", "storedFunction()");
		try 
		{
			session() << "CREATE FUNCTION storedFunction() RETURNS INTEGER AS '"
				"BEGIN "
				" return -1; "
				"END;'"
				"LANGUAGE 'plpgsql'", now;
		}
		catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail (func); }
		catch(StatementException& se){ std::cout << se.toString() << std::endl; fail (func); }

		int i = 0;
		session() << "{? = call storedFunction()}", out(i), now;
		assert(-1 == i);
		dropObject("FUNCTION", "storedFunction()");

		try 
		{
			session() << "CREATE FUNCTION storedFunction(INTEGER) RETURNS INTEGER AS '"
				"BEGIN "
				" RETURN $1 * $1; "
				"END;'"
				"LANGUAGE 'plpgsql'" , now;
		}
		catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail (func); }
		catch(StatementException& se){ std::cout << se.toString() << std::endl; fail (func); }

		i = 2;
		int result = 0;
		session() << "{? = call storedFunction(?)}", out(result), in(i), now;
		assert(4 == result);
		dropObject("FUNCTION", "storedFunction(INTEGER)");

		dropObject("FUNCTION", "storedFunction(TIMESTAMP)");
		try 
		{
			session() << "CREATE FUNCTION storedFunction(TIMESTAMP) RETURNS TIMESTAMP AS '"
				"BEGIN "
				" RETURN $1; "
				"END;'"
				"LANGUAGE 'plpgsql'" , now;
		}
		catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail (func); }
		catch(StatementException& se){ std::cout << se.toString() << std::endl; fail (func); }

		DateTime dtIn(1965, 6, 18, 5, 35, 1);
		DateTime dtOut;
		session() << "{? = call storedFunction(?)}", out(dtOut), in(dtIn), now;
		assert(dtOut == dtIn);
		dropObject("FUNCTION", "storedFunction(TIMESTAMP)");

		dropObject("FUNCTION", "storedFunction(TEXT, TEXT)");
		try 
		{
			session() << "CREATE FUNCTION storedFunction(TEXT,TEXT) RETURNS TEXT AS '"
				"BEGIN "
				" RETURN $1 || '', '' || $2 || ''!'';"
				"END;'"
				"LANGUAGE 'plpgsql'" , now; 
		}
		catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail (func); }
		catch(StatementException& se){ std::cout << se.toString() << std::endl; fail (func); }
		
		std::string param1 = "Hello";
		std::string param2 = "world";
		std::string ret;
		try 
		{
			session() << "{? = call storedFunction(?,?)}", out(ret), in(param1), in(param2), now; 
		}
		catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail (func); }
		catch(StatementException& se){ std::cout << se.toString() << std::endl; fail (func); }

		assert(ret == "Hello, world!");
		dropObject("FUNCTION", "storedFunction(TEXT, TEXT)");

		k += 2;
	}
}


void ODBCPostgreSQLTest::testStoredFunctionAny()
{
	session() << "CREATE FUNCTION storedFunction(INTEGER) RETURNS INTEGER AS '"
			"BEGIN "
			" RETURN $1 * $1; "
			"END;'"
			"LANGUAGE 'plpgsql'" , now;

	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		Any i = 2;
		Any result = 0;
		session() << "{? = call storedFunction(?)}", out(result), in(i), now;
		assert(4 == AnyCast<int>(result));

		k += 2;
	}

	dropObject("FUNCTION", "storedFunction(INTEGER)");
}


void ODBCPostgreSQLTest::testStoredFunctionDynamicAny()
{
	session() << "CREATE FUNCTION storedFunction(INTEGER) RETURNS INTEGER AS '"
			"BEGIN "
			" RETURN $1 * $1; "
			"END;'"
			"LANGUAGE 'plpgsql'" , now;

	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		DynamicAny i = 2;
		DynamicAny result = 0;
		session() << "{? = call storedFunction(?)}", out(result), in(i), now;
		assert(4 == result);

		k += 2;
	}

	dropObject("FUNCTION", "storedFunction(INTEGER)");
}


void ODBCPostgreSQLTest::configurePLPgSQL()
{
	try
	{
		session() << format("CREATE FUNCTION plpgsql_call_handler () "
			"RETURNS OPAQUE "
			"AS '%splpgsql.dll' "
			"LANGUAGE 'C';", _libDir), now;
		
		session() << "CREATE LANGUAGE 'plpgsql' "
			"HANDLER plpgsql_call_handler "
			"LANCOMPILER 'PL/pgSQL'", now;

	}catch(StatementException& ex) 
	{  
		if (7 != ex.diagnostics().nativeError(0)) 
			throw;
	}

	return;
}


void ODBCPostgreSQLTest::dropObject(const std::string& type, const std::string& name)
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
			if (7 == it->_nativeError)//(table does not exist)
			{
				ignoreError = true;
				break;
			}
		}

		if (!ignoreError) throw;
	}
}


void ODBCPostgreSQLTest::recreateNullableTable()
{
	dropObject("TABLE", "NullableTest");
	try { *_pSession << "CREATE TABLE NullableTest (EmptyString VARCHAR(30) NULL, EmptyInteger INTEGER NULL, EmptyFloat FLOAT NULL , EmptyDateTime TIMESTAMP NULL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCPostgreSQLTest::recreatePersonTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCPostgreSQLTest::recreatePersonBLOBTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BYTEA)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
}



void ODBCPostgreSQLTest::recreatePersonDateTimeTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Born TIMESTAMP)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void ODBCPostgreSQLTest::recreatePersonDateTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornDate DATE)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTable()"); }
}


void ODBCPostgreSQLTest::recreatePersonTimeTable()
{
	dropObject("TABLE", "Person");
	try { session() << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornTime TIME)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTimeTable()"); }
}


void ODBCPostgreSQLTest::recreateIntsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateIntsTable()"); }
}


void ODBCPostgreSQLTest::recreateStringsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateStringsTable()"); }
}


void ODBCPostgreSQLTest::recreateFloatsTable()
{
	dropObject("TABLE", "Strings");
	try { session() << "CREATE TABLE Strings (str FLOAT)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateFloatsTable()"); }
}


void ODBCPostgreSQLTest::recreateTuplesTable()
{
	dropObject("TABLE", "Tuples");
	try { session() << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER, int10 INTEGER, int11 INTEGER, int12 INTEGER, int13 INTEGER,"
		"int14 INTEGER, int15 INTEGER, int16 INTEGER, int17 INTEGER, int18 INTEGER, int19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateTuplesTable()"); }
}


void ODBCPostgreSQLTest::recreateVectorsTable()
{
	dropObject("TABLE", "Vectors");
	try { session() << "CREATE TABLE Vectors (int0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateVectorsTable()"); }
}


void ODBCPostgreSQLTest::recreateAnysTable()
{
	dropObject("TABLE", "Anys");
	try { session() << "CREATE TABLE Anys (int0 INTEGER, flt0 FLOAT, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateAnysTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateAnysTable()"); }
}


void ODBCPostgreSQLTest::recreateNullsTable(const std::string& notNull)
{
	dropObject("TABLE", "NullTest");
	try { session() << format("CREATE TABLE NullTest (i INTEGER %s, r FLOAT %s, v VARCHAR(30) %s)",
		notNull,
		notNull,
		notNull), now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCPostgreSQLTest::recreateBoolTable()
{
	dropObject("TABLE", "BoolTest");
	try { session() << "CREATE TABLE BoolTest (b BOOLEAN)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateBoolTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateBoolTable()"); }
}


void ODBCPostgreSQLTest::recreateMiscTable()
{
	dropObject("TABLE", "MiscTest");
	try 
	{ 
		// Mammoth does not bind columns properly
		session() << "CREATE TABLE MiscTest "
			"(First VARCHAR(30),"
			"Second BYTEA,"
			"Third INTEGER,"
			"Fourth FLOAT,"
			"Fifth TIMESTAMP)", now; 
	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateMiscTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateMiscTable()"); }
}


void ODBCPostgreSQLTest::recreateLogTable()
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
			"DateTime TIMESTAMP)"; 

		session() << sql, "T_POCO_LOG", now; 
		session() << sql, "T_POCO_LOG_ARCHIVE", now; 

	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateLogTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateLogTable()"); }
}


void ODBCPostgreSQLTest::recreateUnicodeTable()
{
#if defined (POCO_ODBC_UNICODE)
	dropObject("TABLE", "UnicodeTable");
	try { session() << "CREATE TABLE UnicodeTable (str TEXT)", now; }
	catch (ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail("recreateUnicodeTable()"); }
	catch (StatementException& se){ std::cout << se.toString() << std::endl; fail("recreateUnicodeTable()"); }
#endif
}


CppUnit::Test* ODBCPostgreSQLTest::suite()
{
	if ((_pSession = init(_driver, _dsn, _uid, _pwd, _connectString)))
	{
		std::cout << "*** Connected to [" << _driver << "] test database." << std::endl;

		_pExecutor = new SQLExecutor(_driver + " SQL Executor", _pSession);

		CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCPostgreSQLTest");

		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBareboneODBC);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testZeroRows);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSimpleAccess);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testComplexType);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSimpleAccessVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSharedPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testAutoPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertEmptyVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSimpleAccessList);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testComplexTypeList);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertList);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertEmptyList);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSimpleAccessDeque);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testComplexTypeDeque);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertDeque);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertEmptyDeque);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testAffectedRows);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertSingleBulk);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInsertSingleBulkVec);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLimit);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLimitOnce);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLimitPrepare);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLimitZero);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testPrepare);
//On Linux, PostgreSQL driver returns SQL_NEED_DATA on SQLExecute (see ODBCStatementImpl::bindImpl() )
//this behavior is not expected and not handled for automatic binding
#ifdef POCO_OS_FAMILY_WINDOWS
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBulk);
#endif
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBulkPerformance);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSetSimple);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSetComplex);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSetComplexUnique);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMultiSetSimple);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMultiSetComplex);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMapComplex);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMapComplexUnique);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMultiMapComplex);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSelectIntoSingle);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSelectIntoSingleStep);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSelectIntoSingleFail);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLowerLimitOk);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testLowerLimitFail);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testCombinedLimits);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testCombinedIllegalLimits);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testRange);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testIllegalRange);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSingleSelect);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testEmptyDB);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBLOB);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBLOBContainer);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testBLOBStmt);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testDate);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testTime);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testDateTime);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testFloat);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testDouble);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testTuple);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testTupleVector);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInternalExtraction);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testFilter);
//On Linux, PostgreSQL driver returns SQL_NEED_DATA on SQLExecute (see ODBCStatementImpl::bindImpl() )
//this behavior is not expected and not handled for automatic binding
#ifdef POCO_OS_FAMILY_WINDOWS
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInternalBulkExtraction);
#endif
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testInternalStorageType);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testStoredFunction);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testStoredFunctionAny);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testStoredFunctionDynamicAny);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testNull);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testRowIterator);
#ifdef POCO_ODBC_USE_MAMMOTH_NG
// psqlODBC driver returns string for bool fields
// even when field is explicitly cast to boolean,
// so this functionality seems to be untestable with it
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testStdVectorBool);
#endif
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testAsync);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testAny);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testDynamicAny);
		//neither pSQL ODBC nor Mammoth drivers support multiple results properly
		//CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testMultipleResults);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSQLChannel);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSQLLogger);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testSessionTransaction);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testTransaction);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testTransactor);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testNullable);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testUnicode);
		CppUnit_addTest(pSuite, ODBCPostgreSQLTest, testReconnect);

		return pSuite;
	}

	return 0;
}
