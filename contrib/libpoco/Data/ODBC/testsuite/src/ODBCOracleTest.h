//
// ODBCOracleTest.h
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCOracleTest.h#4 $
//
// Definition of the ODBCOracleTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ODBCOracleTest_INCLUDED
#define ODBCOracleTest_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "ODBCTest.h"


class ODBCOracleTest: public ODBCTest
	/// Oracle ODBC test class
	/// Tested:
	/// 
	/// Driver		|	DB							| OS
	/// ------------+-------------------------------+------------------------------------------
	///	10.02.00.01	| Oracle9i Release 9.2.0.4.0	| MS Windows XP Professional x64 v.2003/SP1
	/// 10.02.00.01 | Oracle XE Release 10.2.0.1.0	| MS Windows XP Professional x64 v.2003/SP1
	/// 11.02.00.02 | Oracle XE Release 11.2.0.2.0	| MS Windows 7  Professional x64 v.2009/SP1
{
public:
	ODBCOracleTest(const std::string& name);
	~ODBCOracleTest();

	void testBareboneODBC();

	void testBLOB();

	void testMultipleResults();

	void testStoredProcedure();
	void testCursorStoredProcedure();
	void testStoredFunction();
	void testCursorStoredFunction();
	void testStoredProcedureAny();
	void testStoredProcedureDynamicAny();
	void testAutoTransaction();

	void testNull();
	static CppUnit::Test* suite();

private:
	static void testBarebone();
	
	void dropObject(const std::string& type, const std::string& name);
	void recreateNullableTable();
	void recreatePersonTable();
	void recreatePersonTupleTable();
	void recreatePersonBLOBTable();
	void recreatePersonDateTable();
	void recreatePersonDateTimeTable();
	void recreateStringsTable();
	void recreateIntsTable();
	void recreateFloatsTable();
	void recreateTuplesTable();
	void recreateVectorsTable();
	void recreateAnysTable();
	void recreateNullsTable(const std::string& notNull = "");
	void recreateMiscTable();
	void recreateLogTable();
	void recreateUnicodeTable();

	static ODBCTest::SessionPtr _pSession;
	static ODBCTest::ExecPtr    _pExecutor;
	
	static std::string _driver;
	static std::string _dsn;
	static std::string _uid;
	static std::string _pwd;
	static std::string _connectString;

	static const std::string MULTI_INSERT;
	static const std::string MULTI_SELECT;
};


inline void ODBCOracleTest::testBareboneODBC()
{
	return testBarebone();
}


#endif // ODBCOracleTest_INCLUDED
