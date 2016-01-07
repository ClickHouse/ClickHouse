//
// ODBCSQLiteTest.h
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCSQLiteTest.h#4 $
//
// Definition of the ODBCSQLiteTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ODBCSQLiteTest_INCLUDED
#define ODBCSQLiteTest_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "ODBCTest.h"


class ODBCSQLiteTest: public ODBCTest
	/// SQLite3 ODBC test class
	/// Tested:
	/// 
	/// Driver		|	DB			| OS
	/// ------------+---------------+------------------------------------------
	///	00.70.00.00	| SQLite 3.*	| MS Windows XP Professional x64 v.2003/SP1
{
public:
	ODBCSQLiteTest(const std::string& name);
	~ODBCSQLiteTest();

	void testBareboneODBC();
	void testAffectedRows();
	void testNull();

	static CppUnit::Test* suite();

private:
	void dropObject(const std::string& type, const std::string& name);
	void recreateNullableTable();
	void recreatePersonTable();
	void recreatePersonBLOBTable();
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

	static ODBCTest::SessionPtr _pSession;
	static ODBCTest::ExecPtr    _pExecutor;
	static std::string          _driver;
	static std::string          _dsn;
	static std::string          _uid;
	static std::string          _pwd;
	static std::string          _connectString;
};


#endif // ODBCSQLiteTest_INCLUDED
