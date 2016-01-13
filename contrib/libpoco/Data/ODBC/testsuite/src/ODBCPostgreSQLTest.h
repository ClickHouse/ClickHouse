//
// ODBCPostgreSQLTest.h
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCPostgreSQLTest.h#4 $
//
// Definition of the ODBCPostgreSQLTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ODBCPostgreSQLTest_INCLUDED
#define ODBCPostgreSQLTest_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "ODBCTest.h"


// uncomment to use Mammoth ODBCng driver
//#define POCO_ODBC_USE_MAMMOTH_NG


class ODBCPostgreSQLTest: public ODBCTest
	/// PostgreSQL ODBC test class
	/// Tested:
	/// 
	/// Driver		   |	DB                | OS                                        | Driver Manager     |Notes
	/// ---------------+----------------------+-------------------------------------------+--------------------+--------------------------
	///	07.03.02.60	   | PostgreSQL 7.4.6     | MS Windows XP Professional x64 v.2003/SP1 | 3.526.3959.0       | BLOB fails (missing 'lo')
	/// 08.01.02.00    | PostgreSQL 8.1.5-1   | MS Windows XP Professional x64 v.2003/SP1 | 3.526.3959.0       |
	/// 1:08.01.0200-2 | PostgreSQL 8.1.5-1   | Ubuntu 7.04 (2.6.20-15-generic #2 SMP)    | unixODBC 2.2.11.-13|
	/// Mammoth ODBCng |                      |                                           |                    | 
	/// (0.99.00.122)  | PostgreSQL 8.1.5-1   | MS Windows XP Professional x64 v.2003/SP1 | 3.526.3959.0       |
	/// 
{
public:
	ODBCPostgreSQLTest(const std::string& name);
	~ODBCPostgreSQLTest();

	void testBareboneODBC();

	void testBLOB();

	void testStoredFunction();
	void testStoredFunctionAny();
	void testStoredFunctionDynamicAny();

	static CppUnit::Test* suite();

private:
	void dropObject(const std::string& type, const std::string& name);
	void recreateNullableTable();
	void recreatePersonTable();
	void recreatePersonBLOBTable();
	void recreatePersonDateTimeTable();
	void recreatePersonDateTable();
	void recreatePersonTimeTable();
	void recreateStringsTable();
	void recreateIntsTable();
	void recreateFloatsTable();
	void recreateTuplesTable();
	void recreateVectorsTable();
	void recreateAnysTable();
	void recreateNullsTable(const std::string& notNull="");
	void recreateBoolTable();
	void recreateMiscTable();
	void recreateLogTable();
	void recreateUnicodeTable();

	void configurePLPgSQL();
		/// Configures PL/pgSQL in the database. A reasonable defaults
		/// for the interpreter location on WIN32 and POSIX platforms are 
		/// supplied (see installDir member variable). 
		/// If these do not work, user must determine the proper location,
		/// modify the function and recompile.
		/// Alternative is direct database configuration for PL/pgSQL usage.

	static const std::string _libDir;
		/// Varible determining the location of the library directory
		/// on the database installation system.
		/// Used to enable PLpgSQL language programmaticaly when
		/// it is not enabled.

	static SessionPtr  _pSession;
	static ExecPtr     _pExecutor;
	static std::string _driver;
	static std::string _dsn;
	static std::string _uid;
	static std::string _pwd;
	static std::string _connectString;
};


#endif // ODBCPostgreSQLTest_INCLUDED
