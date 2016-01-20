//
// SQLiteTest.h
//
// $Id: //poco/Main/Data/SQLite/testsuite/src/SQLiteTest.h#4 $
//
// Definition of the SQLiteTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLiteTest_INCLUDED
#define SQLiteTest_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "CppUnit/TestCase.h"


namespace Poco {
namespace Data {

class Session;

} }


class SQLiteTest: public CppUnit::TestCase
{
public:
	SQLiteTest(const std::string& name);
	~SQLiteTest();

	void testBinding();
	void testZeroRows();
	void testSimpleAccess();
	void testInMemory();
	void testNullCharPointer();
	void testInsertCharPointer();
	void testInsertCharPointer2();
	void testComplexType();
	void testSimpleAccessVector();
	void testComplexTypeVector();
	void testSharedPtrComplexTypeVector();
	void testInsertVector();
	void testInsertEmptyVector();
	void testAffectedRows();
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

	void testCLOB();

	void testTuple1();
	void testTupleVector1();
	void testTuple2();
	void testTupleVector2();
	void testTuple3();
	void testTupleVector3();
	void testTuple4();
	void testTupleVector4();
	void testTuple5();
	void testTupleVector5();
	void testTuple6();
	void testTupleVector6();
	void testTuple7();
	void testTupleVector7();
	void testTuple8();
	void testTupleVector8();
	void testTuple9();
	void testTupleVector9();
	void testTuple10();
	void testTupleVector10();

	void testDateTime();

	void testInternalExtraction();
	void testPrimaryKeyConstraint();
	void testNullable();
	void testNulls();
	void testRowIterator();
	void testAsync();

	void testAny();
	void testDynamicAny();
	void testPair();

	void testSQLChannel();
	void testSQLLogger();

	void testExternalBindingAndExtraction();
	void testBindingCount();
	void testMultipleResults();

	void testReconnect();

	void testThreadModes();

	void testUpdateCallback();
	void testCommitCallback();
	void testRollbackCallback();
	void testNotifier();

	void testSessionTransaction();
	void testTransaction();
	void testTransactor();

	void testFTS3();

	void setUp();
	void tearDown();

	static void sqliteUpdateCallbackFn(void*, int, const char*, const char*, Poco::Int64);
	static int sqliteCommitCallbackFn(void*);
	static void sqliteRollbackCallbackFn(void*);

	void onInsert(const void* pSender);
	void onUpdate(const void* pSender);
	void onDelete(const void* pSender);
	void onCommit(const void* pSender);
	void onRollback(const void* pSender);

	static CppUnit::Test* suite();

private:
	void setTransactionIsolation(Poco::Data::Session& session, Poco::UInt32 ti);

	static int _insertCounter;
	static int _updateCounter;
	static int _deleteCounter;

	int _commitCounter;
	int _rollbackCounter;
};


#endif // SQLiteTest_INCLUDED
