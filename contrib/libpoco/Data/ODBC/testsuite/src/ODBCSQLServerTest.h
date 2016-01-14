//
// ODBCSQLServerTest.h
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCSQLServerTest.h#4 $
//
// Definition of the ODBCSQLServerTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ODBCSQLServerTest_INCLUDED
#define ODBCSQLServerTest_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "ODBCTest.h"


// uncomment to use native SQL Server ODBC driver
// #define POCO_ODBC_USE_SQL_NATIVE


class ODBCSQLServerTest: public ODBCTest
	/// SQLServer ODBC test class
	/// Tested:
	/// 
	/// Driver				|	DB								| OS
	/// --------------------+-----------------------------------+------------------------------------------
	/// 2000.86.1830.00		| SQL Server Express 9.0.2047		| MS Windows XP Professional x64 v.2003/SP1
	/// 2005.90.2047.00		| SQL Server Express 9.0.2047		| MS Windows XP Professional x64 v.2003/SP1
	/// 2009.100.1600.01	| SQL Server Express 10.50.1600.1	| MS Windows XP Professional x64 v.2003/SP1
	///

{
public:
	ODBCSQLServerTest(const std::string& name);
	~ODBCSQLServerTest();

	void testBareboneODBC();

	void testBLOB();
	void testNull();
	void testBulk();

	void testStoredProcedure();
	void testCursorStoredProcedure();
	void testStoredProcedureAny();
	void testStoredProcedureDynamicAny();
	
	void testStoredFunction();

	static CppUnit::Test* suite();

private:
	void dropObject(const std::string& type, const std::string& name);
	void recreateNullableTable();
	void recreatePersonTable();
	void recreatePersonBLOBTable();
	void recreatePersonDateTimeTable();
	void recreatePersonDateTable() { /* no-op */ };
	void recreatePersonTimeTable() { /* no-op */ };
	void recreateStringsTable();
	void recreateIntsTable();
	void recreateFloatsTable();
	void recreateTuplesTable();
	void recreateVectorTable();
	void recreateVectorsTable();
	void recreateAnysTable();
	void recreateNullsTable(const std::string& notNull = "");
	void recreateBoolTable();
	void recreateMiscTable();
	void recreateLogTable();
	void recreateUnicodeTable();

	static SessionPtr  _pSession;
	static ExecPtr     _pExecutor;
	static std::string _driver;
	static std::string _dsn;
	static std::string _uid;
	static std::string _pwd;
	static std::string _db;
	static std::string _connectString;
};


#endif // ODBCSQLServerTest_INCLUDED
