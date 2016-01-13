//
// ODBCMySQLTest.h
//
// $Id: //poco/1.4/Data/MySQL/testsuite/src/ODBCMySQLTest.h#1 $
//
// Definition of the MySQLTest class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MySQLTest_INCLUDED
#define MySQLTest_INCLUDED


#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/Session.h"
#include "Poco/SharedPtr.h"
#include "CppUnit/TestCase.h"
#include "SQLExecutor.h"


class MySQLTest: public CppUnit::TestCase
	/// MySQL test class
	/// Tested:
	/// 
	/// Driver          |            DB             | OS
	/// ----------------+---------------------------+------------------------------------------
	/// 03.51.12.00     | MySQL 5.0.27-community-nt	| MS Windows XP Professional x64 v.2003/SP1
	///                 |                           |
	///                 | Ver 14.14 Distrib 5.5.37, | Linux debian 3.2.0-4-amd64 #1
	///                 | for debian-linux-gnu      | SMP Debian 3.2.57-3 x86_64 GNU/Linux
	///                 | (x86_64) using readline   |
	///                 | 6.2                       |
	///                 |                           |
{
public:
	MySQLTest(const std::string& name);
	~MySQLTest();

	void testBareboneMySQL();

	void testSimpleAccess();
	void testComplexType();
	void testSimpleAccessVector();
	void testComplexTypeVector();
	void testInsertVector();
	void testInsertEmptyVector();

	void testInsertSingleBulk();
	void testInsertSingleBulkVec();

	void testLimit();
	void testLimitOnce();
	void testLimitPrepare();
	void testLimitZero();
	void testPrepare();

	void testSetSimple();
	void testSetComplex();
	void testSetComplexUnique();
	void testMultiSetSimple();
	void testMultiSetComplex();
	void testMapComplex();
	void testMapComplexUnique();
	void testMultiMapComplex();
	void testSelectIntoSingle();
	void testSelectIntoSingleStep();
	void testSelectIntoSingleFail();
	void testLowerLimitOk();
	void testLowerLimitFail();
	void testCombinedLimits();
	void testCombinedIllegalLimits();
	void testRange();
	void testIllegalRange();
	void testSingleSelect();
	void testEmptyDB();
	void testDateTime();
	void testBLOB();
	void testBLOBStmt();

	void testUnsignedInts();
	void testFloat();
	void testDouble();

	void testTuple();
	void testTupleVector();

	void testInternalExtraction();

	void testNull();
	void testNullVector();

	void testNullableInt();
	void testNullableString();
	void testTupleWithNullable();

	void testSessionTransaction();
	void testTransaction();

	void testReconnect();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	static void connectNoDB();

	void dropTable(const std::string& tableName);
	void recreatePersonTable();
	void recreatePersonBLOBTable();
	void recreatePersonDateTimeTable();
	void recreatePersonDateTable();
	void recreatePersonTimeTable();
	void recreateStringsTable();
	void recreateIntsTable();
	void recreateUnsignedIntsTable();
	void recreateFloatsTable();
	void recreateTuplesTable();
	void recreateVectorsTable();
	void recreateNullableIntTable();
	void recreateNullableStringTable();

	static void dbInfo(Poco::Data::Session& session);

	static std::string _dbConnString;
	static Poco::SharedPtr<Poco::Data::Session> _pSession;
	static Poco::SharedPtr<SQLExecutor> _pExecutor;
	static const bool bindValues[8];
};


#endif // MySQLTest_INCLUDED
