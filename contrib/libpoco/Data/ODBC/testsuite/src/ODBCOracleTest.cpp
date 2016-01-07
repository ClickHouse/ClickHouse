//
// ODBCOracleTest.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCOracleTest.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCOracleTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Tuple.h"
#include "Poco/Format.h"
#include "Poco/Any.h"
#include "Poco/DynamicAny.h"
#include "Poco/DateTime.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/AutoTransaction.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Data/ODBC/ODBCException.h"


using namespace Poco::Data::Keywords;
using Poco::Data::DataException;
using Poco::Data::Statement;
using Poco::Data::RecordSet;
using Poco::Data::AutoTransaction;
using Poco::Data::Session;
using Poco::Data::ODBC::Utility;
using Poco::Data::ODBC::ConnectionException;
using Poco::Data::ODBC::StatementException;
using Poco::Data::ODBC::StatementDiagnostics;
using Poco::format;
using Poco::Tuple;
using Poco::Any;
using Poco::AnyCast;
using Poco::DynamicAny;
using Poco::DateTime;


#define ORACLE_ODBC_DRIVER "Oracle in XE"
#define ORACLE_DSN "PocoDataOracleTest"
#define ORACLE_SERVER POCO_ODBC_TEST_DATABASE_SERVER
#define ORACLE_PORT "1521"
#define ORACLE_SID "XE"
#define ORACLE_UID "poco"
#define ORACLE_PWD "poco"


ODBCTest::SessionPtr ODBCOracleTest::_pSession;
ODBCTest::ExecPtr    ODBCOracleTest::_pExecutor;
std::string          ODBCOracleTest::_driver = ORACLE_ODBC_DRIVER;
std::string          ODBCOracleTest::_dsn = ORACLE_DSN;
std::string          ODBCOracleTest::_uid = ORACLE_UID;
std::string          ODBCOracleTest::_pwd = ORACLE_PWD;
std::string          ODBCOracleTest::_connectString = "DRIVER={" ORACLE_ODBC_DRIVER "};"
	"DBQ=" ORACLE_SERVER ":" ORACLE_PORT "/" ORACLE_SID ";"
	"UID=" ORACLE_UID ";"
	"PWD=" ORACLE_PWD ";"
	"TLO=O;" // translation option
	"FBS=60000;" // fetch buffer size (bytes), default 60000
	"FWC=F;" // force SQL_WCHAR support (T/F), default F
	"CSR=F;" // close cursor (T/F), default F
	"MDI=T;" // metadata ID (SQL_ATTR_METADATA_ID) (T/F), default T
	"MTS=F;" // Microsoft Transaction Server support (T/F)
	"DPM=F;" // disable SQLDescribeParam (T/F), default F
	"NUM=NLS;" // numeric settings (NLS implies Globalization Support)
	"BAM=IfAllSuccessful;" // batch autocommit, (IfAllSuccessful/UpToFirstFailure/AllSuccessful), default IfAllSuccessful
	"BTD=F;" // bind timestamp as date (T/F), default F
	"RST=T;" // resultsets (T/F), default T
	"LOB=T;" // LOB writes (T/F), default T
	"FDL=0;" // failover delay (default 10)
	"FRC=0;" // failover retry count (default 10)
	"QTO=T;" // query timout option (T/F), default T
	"FEN=F;" // failover (T/F), default T
	"XSM=Default;" // schema field (Default/Database/Owner), default Default
	"EXC=F;" // EXEC syntax (T/F), default F
	"APA=T;" // thread safety (T/F), default T
	"DBA=W;"; // write access (R/W)

const std::string ODBCOracleTest::MULTI_INSERT = 
	"BEGIN "
	"INSERT INTO Test VALUES ('1', 2, 3.5);"
	"INSERT INTO Test VALUES ('2', 3, 4.5);"
	"INSERT INTO Test VALUES ('3', 4, 5.5);"
	"INSERT INTO Test VALUES ('4', 5, 6.5);"
	"INSERT INTO Test VALUES ('5', 6, 7.5);"
	"END;";

const std::string ODBCOracleTest::MULTI_SELECT =
	"{CALL multiResultsProcedure()}";


ODBCOracleTest::ODBCOracleTest(const std::string& name): 
	ODBCTest(name, _pSession, _pExecutor, _dsn, _uid, _pwd, _connectString)
{
}


ODBCOracleTest::~ODBCOracleTest()
{
}


void ODBCOracleTest::testBarebone()
{
	std::string tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second VARCHAR(30),"
		"Third BLOB,"
		"Fourth INTEGER,"
		"Fifth NUMBER,"
		"Sixth TIMESTAMP)";

	_pExecutor->bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_IMMEDIATE, SQLExecutor::DE_BOUND);
	_pExecutor->bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_MANUAL);
	_pExecutor->bareboneODBCTest(_connectString, tableCreateString, SQLExecutor::PB_AT_EXEC, SQLExecutor::DE_BOUND);

	tableCreateString = "CREATE TABLE Test "
		"(First VARCHAR(30),"
		"Second INTEGER,"
		"Third NUMBER)";

	*_pSession << "CREATE OR REPLACE "
			"PROCEDURE multiResultsProcedure(ret1 OUT SYS_REFCURSOR, "
			"ret2 OUT SYS_REFCURSOR,"
			"ret3 OUT SYS_REFCURSOR,"
			"ret4 OUT SYS_REFCURSOR,"
			"ret5 OUT SYS_REFCURSOR) IS "
			"BEGIN "
			"OPEN ret1 FOR SELECT * FROM Test WHERE First = '1';"
			"OPEN ret2 FOR SELECT * FROM Test WHERE First = '2';"
			"OPEN ret3 FOR SELECT * FROM Test WHERE First = '3';"
			"OPEN ret4 FOR SELECT * FROM Test WHERE First = '4';"
			"OPEN ret5 FOR SELECT * FROM Test WHERE First = '5';"
			"END multiResultsProcedure;" , now;

	_pExecutor->bareboneODBCMultiResultTest(_connectString, 
		tableCreateString, 
		SQLExecutor::PB_IMMEDIATE, 
		SQLExecutor::DE_MANUAL,
		MULTI_INSERT,
		MULTI_SELECT);
	_pExecutor->bareboneODBCMultiResultTest(_connectString, 
		tableCreateString, 
		SQLExecutor::PB_IMMEDIATE, 
		SQLExecutor::DE_BOUND,
		MULTI_INSERT,
		MULTI_SELECT);
	_pExecutor->bareboneODBCMultiResultTest(_connectString, 
		tableCreateString, 
		SQLExecutor::PB_AT_EXEC, 
		SQLExecutor::DE_MANUAL,
		MULTI_INSERT,
		MULTI_SELECT);
	_pExecutor->bareboneODBCMultiResultTest(_connectString, 
		tableCreateString, 
		SQLExecutor::PB_AT_EXEC, 
		SQLExecutor::DE_BOUND,
		MULTI_INSERT,
		MULTI_SELECT);

}


void ODBCOracleTest::testBLOB()
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
		executor().blob(maxFldSize);
		i += 2;
	}

	recreatePersonBLOBTable();
	try
	{
		executor().blob(maxFldSize+1);
		fail ("must fail");
	}
	catch (DataException&) { }
}


void ODBCOracleTest::testNull()
{
	// test for NOT NULL violation exception
	for (int i = 0; i < 8;)
	{
		recreateNullsTable("NOT NULL");
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		executor().notNulls("HY000");
		i += 2;
	}

	// test for null insertion
	for (int i = 0; i < 8;)
	{
		recreateNullsTable();
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		executor().nulls();
		i += 2;
	}
}


void ODBCOracleTest::testStoredProcedure()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(outParam OUT NUMBER) IS "
			" BEGIN outParam := -1; "
			"END storedProcedure;" , now;

		int i = 0;
		*_pSession << "{call storedProcedure(?)}", out(i), now;
		assert(-1 == i);
		dropObject("PROCEDURE", "storedProcedure");

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(inParam IN NUMBER, outParam OUT NUMBER) IS "
			" BEGIN outParam := inParam*inParam; "
			"END storedProcedure;" , now;

		i = 2;
		int j = 0;
		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == j);
		*_pSession << "DROP PROCEDURE storedProcedure;", now;

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(ioParam IN OUT NUMBER) IS "
			" BEGIN ioParam := ioParam*ioParam; "
			" END storedProcedure;" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == i);
		dropObject("PROCEDURE", "storedProcedure");

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(ioParam IN OUT DATE) IS "
			" BEGIN ioParam := ioParam + 1; "
			" END storedProcedure;" , now;

		DateTime dt(1965, 6, 18, 5, 35, 1);
		*_pSession << "{call storedProcedure(?)}", io(dt), now;
		assert(19 == dt.day());
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}

	
	//strings only work with auto-binding
	session().setFeature("autoBind", true);

	*_pSession << "CREATE OR REPLACE "
		"PROCEDURE storedProcedure(inParam IN VARCHAR2, outParam OUT VARCHAR2) IS "
		" BEGIN outParam := inParam; "
		"END storedProcedure;" , now;

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
}


void ODBCOracleTest::testStoredProcedureAny()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		Any i = 2;
		Any j = 0;

		*_pSession << "CREATE OR REPLACE "
				"PROCEDURE storedProcedure(inParam IN NUMBER, outParam OUT NUMBER) IS "
				" BEGIN outParam := inParam*inParam; "
				"END storedProcedure;" , now;

		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == AnyCast<int>(j));
		*_pSession << "DROP PROCEDURE storedProcedure;", now;

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(ioParam IN OUT NUMBER) IS "
			" BEGIN ioParam := ioParam*ioParam; "
			" END storedProcedure;" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == AnyCast<int>(i));
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}
}


void ODBCOracleTest::testStoredProcedureDynamicAny()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		
		DynamicAny i = 2;
		DynamicAny j = 0;

		*_pSession << "CREATE OR REPLACE "
				"PROCEDURE storedProcedure(inParam IN NUMBER, outParam OUT NUMBER) IS "
				" BEGIN outParam := inParam*inParam; "
				"END storedProcedure;" , now;

		*_pSession << "{call storedProcedure(?, ?)}", in(i), out(j), now;
		assert(4 == j);
		*_pSession << "DROP PROCEDURE storedProcedure;", now;

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedProcedure(ioParam IN OUT NUMBER) IS "
			" BEGIN ioParam := ioParam*ioParam; "
			" END storedProcedure;" , now;

		i = 2;
		*_pSession << "{call storedProcedure(?)}", io(i), now;
		assert(4 == i);
		dropObject("PROCEDURE", "storedProcedure");

		k += 2;
	}
}


void ODBCOracleTest::testCursorStoredProcedure()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		recreatePersonTable();
		typedef Tuple<std::string, std::string, std::string, int> Person;
		std::vector<Person> people;
		people.push_back(Person("Simpson", "Homer", "Springfield", 42));
		people.push_back(Person("Simpson", "Bart", "Springfield", 12));
		people.push_back(Person("Simpson", "Lisa", "Springfield", 10));
		*_pSession << "INSERT INTO Person VALUES (?, ?, ?, ?)", use(people), now;

		*_pSession << "CREATE OR REPLACE "
			"PROCEDURE storedCursorProcedure(ret OUT SYS_REFCURSOR, ageLimit IN NUMBER) IS "
			" BEGIN "
			" OPEN ret FOR "
			" SELECT * "
			" FROM Person "
			" WHERE Age < ageLimit " 
			" ORDER BY Age DESC; "
			" END storedCursorProcedure;" , now;

		people.clear();
		int age = 13;
		
		*_pSession << "{call storedCursorProcedure(?)}", in(age), into(people), now;
		
		assert (2 == people.size());
		assert (Person("Simpson", "Bart", "Springfield", 12) == people[0]);
		assert (Person("Simpson", "Lisa", "Springfield", 10) == people[1]);

		Statement stmt = ((*_pSession << "{call storedCursorProcedure(?)}", in(age), now));
		RecordSet rs(stmt);
		assert (rs["LastName"] == "Simpson");
		assert (rs["FirstName"] == "Bart");
		assert (rs["Address"] == "Springfield");
		assert (rs["Age"] == 12);

		dropObject("TABLE", "Person");
		dropObject("PROCEDURE", "storedCursorProcedure");

		k += 2;
	}
}


void ODBCOracleTest::testStoredFunction()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		try{
		*_pSession << "CREATE OR REPLACE "
			"FUNCTION storedFunction RETURN NUMBER IS "
			" BEGIN return(-1); "
			" END storedFunction;" , now;
		}catch(StatementException& se) { std::cout << se.toString() << std::endl; }

		int i = 0;
		*_pSession << "{? = call storedFunction()}", out(i), now;
		assert(-1 == i);
		dropObject("FUNCTION", "storedFunction");


		*_pSession << "CREATE OR REPLACE "
			"FUNCTION storedFunction(inParam IN NUMBER) RETURN NUMBER IS "
			" BEGIN RETURN(inParam*inParam); "
			" END storedFunction;" , now;

		i = 2;
		int result = 0;
		*_pSession << "{? = call storedFunction(?)}", out(result), in(i), now;
		assert(4 == result);
		dropObject("FUNCTION", "storedFunction");

		*_pSession << "CREATE OR REPLACE "
			"FUNCTION storedFunction(inParam IN NUMBER, outParam OUT NUMBER) RETURN NUMBER IS "
			" BEGIN outParam := inParam*inParam; RETURN(outParam); "
			" END storedFunction;" , now;

		i = 2;
		int j = 0;
		result = 0;
		*_pSession << "{? = call storedFunction(?, ?)}", out(result), in(i), out(j), now;
		assert(4 == j);
		assert(j == result); 
		dropObject("FUNCTION", "storedFunction");

		*_pSession << "CREATE OR REPLACE "
			"FUNCTION storedFunction(param1 IN OUT NUMBER, param2 IN OUT NUMBER) RETURN NUMBER IS "
			" temp NUMBER := param1; "
			" BEGIN param1 := param2; param2 := temp; RETURN(param1+param2); "
			" END storedFunction;" , now;

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
		dropObject("FUNCTION", "storedFunction");
		
		k += 2;
	}

	session().setFeature("autoBind", true);

	*_pSession << "CREATE OR REPLACE "
		"FUNCTION storedFunction(inParam IN VARCHAR2, outParam OUT VARCHAR2) RETURN VARCHAR2 IS "
		" BEGIN outParam := inParam; RETURN outParam;"
		"END storedFunction;" , now;

	std::string inParam = "123";
	std::string outParam;
	std::string ret;
	*_pSession << "{? = call storedFunction(?,?)}", out(ret), in(inParam), out(outParam), now;
	assert("123" == inParam);
	assert(inParam == outParam);
	assert(ret == outParam);
	dropObject("PROCEDURE", "storedFunction");
}


void ODBCOracleTest::testCursorStoredFunction()
{
	for (int k = 0; k < 8;)
	{
		session().setFeature("autoBind", bindValue(k));
		session().setFeature("autoExtract", bindValue(k+1));

		recreatePersonTable();
		typedef Tuple<std::string, std::string, std::string, int> Person;
		std::vector<Person> people;
		people.push_back(Person("Simpson", "Homer", "Springfield", 42));
		people.push_back(Person("Simpson", "Bart", "Springfield", 12));
		people.push_back(Person("Simpson", "Lisa", "Springfield", 10));
		*_pSession << "INSERT INTO Person VALUES (?, ?, ?, ?)", use(people), now;

		*_pSession << "CREATE OR REPLACE "
			"FUNCTION storedCursorFunction(ageLimit IN NUMBER) RETURN SYS_REFCURSOR IS "
			" ret SYS_REFCURSOR; "
			" BEGIN "
			" OPEN ret FOR "
			" SELECT * "
			" FROM Person "
			" WHERE Age < ageLimit " 
			" ORDER BY Age DESC; "
			" RETURN ret; "
			" END storedCursorFunction;" , now;

		people.clear();
		int age = 13;
		
		*_pSession << "{call storedCursorFunction(?)}", in(age), into(people), now;
		
		assert (2 == people.size());
		assert (Person("Simpson", "Bart", "Springfield", 12) == people[0]);
		assert (Person("Simpson", "Lisa", "Springfield", 10) == people[1]);

		Statement stmt = ((*_pSession << "{call storedCursorFunction(?)}", in(age), now));
		RecordSet rs(stmt);
		assert (rs["LastName"] == "Simpson");
		assert (rs["FirstName"] == "Bart");
		assert (rs["Address"] == "Springfield");
		assert (rs["Age"] == 12);

		dropObject("TABLE", "Person");
		dropObject("FUNCTION", "storedCursorFunction");
		
		k += 2;
	}
}


void ODBCOracleTest::testMultipleResults()
{
	std::string sql = "CREATE OR REPLACE "
		"PROCEDURE multiResultsProcedure(paramAge1 IN NUMBER,"
		" paramAge2 IN NUMBER,"
		" paramAge3 IN NUMBER,"
		" ret1 OUT SYS_REFCURSOR, "
		" ret2 OUT SYS_REFCURSOR,"
		" ret3 OUT SYS_REFCURSOR) IS "
		"BEGIN "
		" OPEN ret1 FOR SELECT * FROM Person WHERE Age = paramAge1;"
		" OPEN ret2 FOR SELECT Age FROM Person WHERE FirstName = 'Bart';"
		" OPEN ret3 FOR SELECT * FROM Person WHERE Age = paramAge2 OR Age = paramAge3 ORDER BY Age;"
		"END multiResultsProcedure;";

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		*_pSession << sql, now;
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		executor().multipleResults("{call multiResultsProcedure(?, ?, ?)}");

		i += 2;
	}
}


void ODBCOracleTest::testAutoTransaction()
{
	Session localSession("ODBC", _connectString);
	bool ac = session().getFeature("autoCommit");
	int count = 0;

	recreateIntsTable();

	session().setFeature("autoCommit", true);
	session() << "INSERT INTO Strings VALUES (1)", now;
	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (1 == count);
	session() << "INSERT INTO Strings VALUES (2)", now;
	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (2 == count);
	session() << "INSERT INTO Strings VALUES (3)", now;
	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (3 == count);

	session() << "DELETE FROM Strings", now;
	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (0 == count);

	session().setFeature("autoCommit", false);
	
	try
	{
		AutoTransaction at(session());
		session() << "INSERT INTO Strings VALUES (1)", now;
		session() << "INSERT INTO Strings VALUES (2)", now;
		session() << "BAD QUERY", now;
	} catch (Poco::Exception&) {}

	session() << "SELECT count(*) FROM Strings", into(count), now;
	assert (0 == count);

	AutoTransaction at(session());

	session() << "INSERT INTO Strings VALUES (1)", now;
	session() << "INSERT INTO Strings VALUES (2)", now;
	session() << "INSERT INTO Strings VALUES (3)", now;

	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (0 == count);

	at.commit();

	localSession << "SELECT count(*) FROM Strings", into(count), now;
	assert (3 == count);

	session().setFeature("autoCommit", ac);
}


void ODBCOracleTest::dropObject(const std::string& type, const std::string& name)
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
			if (4043 == it->_nativeError || //ORA-04043 (object does not exist)
				942 == it->_nativeError)//ORA-00942 (table does not exist)
			{
				ignoreError = true;
				break;
			}
		}

		if (!ignoreError) throw;
	}
}


void ODBCOracleTest::recreateNullableTable()
{
	dropObject("TABLE", "NullableTest");
	try { *_pSession << "CREATE TABLE NullableTest (EmptyString VARCHAR2(30) NULL, EmptyInteger INTEGER NULL, EmptyFloat NUMBER NULL , EmptyDateTime TIMESTAMP NULL)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCOracleTest::recreatePersonTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR2(30), FirstName VARCHAR2(30), Address VARCHAR2(30), Age INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTable()"); }
}


void ODBCOracleTest::recreatePersonTupleTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName1 VARCHAR2(30), FirstName1 VARCHAR2(30), Address1 VARCHAR2(30), Age1 INTEGER,"
		"LastName2 VARCHAR2(30), FirstName2 VARCHAR2(30), Address2 VARCHAR2(30), Age2 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonTupleTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonTupleTable()"); }
}


void ODBCOracleTest::recreatePersonBLOBTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Image BLOB)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonBLOBTable()"); }
}


void ODBCOracleTest::recreatePersonDateTimeTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), Born TIMESTAMP)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTimeTable()"); }
}


void ODBCOracleTest::recreatePersonDateTable()
{
	dropObject("TABLE", "Person");
	try { *_pSession << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR(30), Address VARCHAR(30), BornDate DATE)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreatePersonDateTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreatePersonDateTable()"); }
}


void ODBCOracleTest::recreateIntsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateIntsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateIntsTable()"); }
}


void ODBCOracleTest::recreateStringsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateStringsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateStringsTable()"); }
}


void ODBCOracleTest::recreateFloatsTable()
{
	dropObject("TABLE", "Strings");
	try { *_pSession << "CREATE TABLE Strings (str NUMBER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateFloatsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateFloatsTable()"); }
}


void ODBCOracleTest::recreateTuplesTable()
{
	dropObject("TABLE", "Tuples");
	try { *_pSession << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER, int10 INTEGER, int11 INTEGER, int12 INTEGER, int13 INTEGER,"
		"int14 INTEGER, int15 INTEGER, int16 INTEGER, int17 INTEGER, int18 INTEGER, int19 INTEGER)", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateTuplesTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateTuplesTable()"); }
}


void ODBCOracleTest::recreateVectorsTable()
{
	dropObject("TABLE", "Vectors");
	try { *_pSession << "CREATE TABLE Vectors (int0 INTEGER, flt0 NUMBER(5,2), str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateVectorsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateVectorsTable()"); }
}


void ODBCOracleTest::recreateAnysTable()
{
	dropObject("TABLE", "Anys");
	try { *_pSession << "CREATE TABLE Anys (int0 INTEGER, flt0 NUMBER, str0 VARCHAR(30))", now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateAnysTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateAnysTable()"); }
}


void ODBCOracleTest::recreateNullsTable(const std::string& notNull)
{
	dropObject("TABLE", "NullTest");
	try { *_pSession << format("CREATE TABLE NullTest (i INTEGER %s, r NUMBER %s, v VARCHAR(30) %s)",
		notNull,
		notNull,
		notNull), now; }
	catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateNullsTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateNullsTable()"); }
}


void ODBCOracleTest::recreateMiscTable()
{
	dropObject("TABLE", "MiscTest");
	try 
	{ 
		session() << "CREATE TABLE MiscTest "
			"(First VARCHAR(30),"
			"Second BLOB,"
			"Third INTEGER,"
			"Fourth NUMBER,"
			"Fifth TIMESTAMP)", now; 
	} catch(ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail ("recreateMiscTable()"); }
	catch(StatementException& se){ std::cout << se.toString() << std::endl; fail ("recreateMiscTable()"); }
}


void ODBCOracleTest::recreateLogTable()
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


void ODBCOracleTest::recreateUnicodeTable()
{
#if defined (POCO_ODBC_UNICODE)
	dropObject("TABLE", "UnicodeTable");
	try { session() << "CREATE TABLE UnicodeTable (str NVARCHAR2(30))", now; }
	catch (ConnectionException& ce){ std::cout << ce.toString() << std::endl; fail("recreateUnicodeTable()"); }
	catch (StatementException& se){ std::cout << se.toString() << std::endl; fail("recreateUnicodeTable()"); }
#endif
}


CppUnit::Test* ODBCOracleTest::suite()
{
	if ((_pSession = init(_driver, _dsn, _uid, _pwd, _connectString)))
	{
		std::cout << "*** Connected to [" << _driver << "] test database." << std::endl;

		_pExecutor = new SQLExecutor(_driver + " SQL Executor", _pSession);

		CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCOracleTest");

		CppUnit_addTest(pSuite, ODBCOracleTest, testBareboneODBC);
		CppUnit_addTest(pSuite, ODBCOracleTest, testZeroRows);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSimpleAccess);
		CppUnit_addTest(pSuite, ODBCOracleTest, testComplexType);
		CppUnit_addTest(pSuite, ODBCOracleTest, testComplexTypeTuple);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSimpleAccessVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSharedPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testAutoPtrComplexTypeVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertEmptyVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSimpleAccessList);
		CppUnit_addTest(pSuite, ODBCOracleTest, testComplexTypeList);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertList);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertEmptyList);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSimpleAccessDeque);
		CppUnit_addTest(pSuite, ODBCOracleTest, testComplexTypeDeque);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertDeque);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertEmptyDeque);
		CppUnit_addTest(pSuite, ODBCOracleTest, testAffectedRows);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertSingleBulk);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInsertSingleBulkVec);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLimit);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLimitOnce);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLimitPrepare);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLimitZero);
		CppUnit_addTest(pSuite, ODBCOracleTest, testPrepare);
		CppUnit_addTest(pSuite, ODBCOracleTest, testBulk);
		CppUnit_addTest(pSuite, ODBCOracleTest, testBulkPerformance);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSetSimple);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSetComplex);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSetComplexUnique);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMultiSetSimple);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMultiSetComplex);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMapComplex);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMapComplexUnique);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMultiMapComplex);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSelectIntoSingle);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSelectIntoSingleStep);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSelectIntoSingleFail);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLowerLimitOk);
		CppUnit_addTest(pSuite, ODBCOracleTest, testLowerLimitFail);
		CppUnit_addTest(pSuite, ODBCOracleTest, testCombinedLimits);
		CppUnit_addTest(pSuite, ODBCOracleTest, testCombinedIllegalLimits);
		CppUnit_addTest(pSuite, ODBCOracleTest, testRange);
		CppUnit_addTest(pSuite, ODBCOracleTest, testIllegalRange);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSingleSelect);
		CppUnit_addTest(pSuite, ODBCOracleTest, testEmptyDB);
		CppUnit_addTest(pSuite, ODBCOracleTest, testBLOB);
		CppUnit_addTest(pSuite, ODBCOracleTest, testBLOBContainer);
		CppUnit_addTest(pSuite, ODBCOracleTest, testBLOBStmt);
		CppUnit_addTest(pSuite, ODBCOracleTest, testDate);
		CppUnit_addTest(pSuite, ODBCOracleTest, testDateTime);
		CppUnit_addTest(pSuite, ODBCOracleTest, testFloat);
		CppUnit_addTest(pSuite, ODBCOracleTest, testDouble);
		CppUnit_addTest(pSuite, ODBCOracleTest, testTuple);
		CppUnit_addTest(pSuite, ODBCOracleTest, testTupleVector);
		CppUnit_addTest(pSuite, ODBCOracleTest, testStoredProcedure);
		CppUnit_addTest(pSuite, ODBCOracleTest, testCursorStoredProcedure);
		CppUnit_addTest(pSuite, ODBCOracleTest, testStoredProcedureAny);
		CppUnit_addTest(pSuite, ODBCOracleTest, testStoredProcedureDynamicAny);
		CppUnit_addTest(pSuite, ODBCOracleTest, testStoredFunction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testCursorStoredFunction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInternalExtraction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testFilter);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInternalBulkExtraction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testInternalStorageType);
		CppUnit_addTest(pSuite, ODBCOracleTest, testNull);
		CppUnit_addTest(pSuite, ODBCOracleTest, testRowIterator);
		CppUnit_addTest(pSuite, ODBCOracleTest, testAsync);
		CppUnit_addTest(pSuite, ODBCOracleTest, testAny);
		CppUnit_addTest(pSuite, ODBCOracleTest, testDynamicAny);
		CppUnit_addTest(pSuite, ODBCOracleTest, testMultipleResults);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSQLChannel);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSQLLogger);
		CppUnit_addTest(pSuite, ODBCOracleTest, testAutoTransaction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testSessionTransaction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testTransaction);
		CppUnit_addTest(pSuite, ODBCOracleTest, testTransactor);
		CppUnit_addTest(pSuite, ODBCOracleTest, testNullable);
		CppUnit_addTest(pSuite, ODBCOracleTest, testUnicode);
		CppUnit_addTest(pSuite, ODBCOracleTest, testReconnect);

		return pSuite;
	}

	return 0;
}
