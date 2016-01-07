//
// ODBCTest.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCTest.cpp#5 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/String.h"
#include "Poco/Format.h"
#include "Poco/Any.h"
#include "Poco/DynamicAny.h"
#include "Poco/Tuple.h"
#include "Poco/DateTime.h"
#include "Poco/Exception.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/ODBC/Connector.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Data/ODBC/ODBCStatementImpl.h"
#include "Poco/Data/DataException.h"
#include <sqltypes.h>
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::ConnectionFailedException;
using Poco::Data::CLOB;
using Poco::Data::ODBC::Utility;
using Poco::Data::ODBC::ODBCException;
using Poco::Data::ODBC::ConnectionException;
using Poco::Data::ODBC::StatementException;
using Poco::Data::ODBC::StatementDiagnostics;
using Poco::format;
using Poco::Tuple;
using Poco::Any;
using Poco::AnyCast;
using Poco::DynamicAny;
using Poco::DateTime;
using Poco::NotFoundException;


ODBCTest::Drivers ODBCTest::_drivers;
const bool        ODBCTest::_bindValues[8] = 
	{true, true, true, false, false, true, false, false};


ODBCTest::ODBCTest(const std::string& name,
	SessionPtr pSession,
	ExecPtr    pExecutor,
	std::string& rDSN,
	std::string& rUID,
	std::string& rPwd,
	std::string& rConnectString): 
	CppUnit::TestCase(name),
	_pSession(pSession),
	_pExecutor(pExecutor),
	_rDSN(rDSN),
	_rUID(rUID),
	_rPwd(rPwd),
	_rConnectString(rConnectString)
{
	_pSession->setFeature("autoCommit", true);
}


ODBCTest::~ODBCTest()
{
}


void ODBCTest::testZeroRows()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableName("Person");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->zeroRows();
		i += 2;
	}
}


void ODBCTest::testSimpleAccess()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableName("Person");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->simpleAccess();
		i += 2;
	}
}


void ODBCTest::testComplexType()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->complexType();
		i += 2;
	}
}


void ODBCTest::testComplexTypeTuple()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTupleTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->complexTypeTuple();
		i += 2;
	}
}


void ODBCTest::testSimpleAccessVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->simpleAccessVector();
		i += 2;
	}
}


void ODBCTest::testComplexTypeVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->complexTypeVector();
		i += 2;
	}
}


void ODBCTest::testSharedPtrComplexTypeVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->sharedPtrComplexTypeVector();
		i += 2;
	}
}


void ODBCTest::testAutoPtrComplexTypeVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->autoPtrComplexTypeVector();
		i += 2;
	}
}


void ODBCTest::testInsertVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertVector();
		i += 2;
	}	
}


void ODBCTest::testInsertEmptyVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertEmptyVector();
		i += 2;
	}	
}


void ODBCTest::testSimpleAccessList()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->simpleAccessList();
		i += 2;
	}
}


void ODBCTest::testComplexTypeList()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->complexTypeList();
		i += 2;
	}	
}


void ODBCTest::testInsertList()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertList();
		i += 2;
	}	
}


void ODBCTest::testInsertEmptyList()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertEmptyList();
		i += 2;
	}	
}


void ODBCTest::testSimpleAccessDeque()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->simpleAccessDeque();
		i += 2;
	}
}


void ODBCTest::testComplexTypeDeque()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->complexTypeDeque();
		i += 2;
	}	
}


void ODBCTest::testInsertDeque()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertDeque();
		i += 2;
	}	
}


void ODBCTest::testInsertEmptyDeque()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertEmptyDeque();
		i += 2;
	}	
}


void ODBCTest::testAffectedRows()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateStringsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->affectedRows();
		i += 2;
	}	
}


void ODBCTest::testInsertSingleBulk()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertSingleBulk();
		i += 2;
	}	
}


void ODBCTest::testInsertSingleBulkVec()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->insertSingleBulkVec();
		i += 2;
	}	
}


void ODBCTest::testLimit()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->limits();
		i += 2;
	}
}


void ODBCTest::testLimitZero()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->limitZero();
		i += 2;
	}	
}


void ODBCTest::testLimitOnce()
{
	if (!_pSession) fail ("Test not available.");

	recreateIntsTable();
	_pExecutor->limitOnce();
	
}


void ODBCTest::testLimitPrepare()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->limitPrepare();
		i += 2;
	}
}



void ODBCTest::testPrepare()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->prepare();
		i += 2;
	}
}


void ODBCTest::testBulk()
{
	if (!_pSession) fail ("Test not available.");

	_pSession->setFeature("autoBind", true);
	_pSession->setFeature("autoExtract", true);

	recreateMiscTable();
	_pExecutor->doBulk<std::vector<int>,
		std::vector<std::string>,
		std::vector<CLOB>,
		std::vector<double>,
		std::vector<DateTime> >(100);

	recreateMiscTable();
	_pExecutor->doBulk<std::deque<int>,
		std::deque<std::string>,
		std::deque<CLOB>,
		std::deque<double>,
		std::deque<DateTime> >(100);

	recreateMiscTable();
	_pExecutor->doBulk<std::list<int>,
		std::list<std::string>,
		std::list<CLOB>,
		std::list<double>,
		std::list<DateTime> >(100);
}


void ODBCTest::testBulkPerformance()
{
	if (!_pSession) fail ("Test not available.");

	_pSession->setFeature("autoBind", true);
	_pSession->setFeature("autoExtract", true);

	recreateMiscTable();
	_pExecutor->doBulkPerformance(1000);
}


void ODBCTest::testSetSimple()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->setSimple();
		i += 2;
	}
}


void ODBCTest::testSetComplex()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->setComplex();
		i += 2;
	}
}


void ODBCTest::testSetComplexUnique()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->setComplexUnique();
		i += 2;
	}
}

void ODBCTest::testMultiSetSimple()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->multiSetSimple();
		i += 2;
	}
}


void ODBCTest::testMultiSetComplex()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->multiSetComplex();
		i += 2;
	}	
}


void ODBCTest::testMapComplex()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->mapComplex();
		i += 2;
	}
}


void ODBCTest::testMapComplexUnique()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->mapComplexUnique();
		i += 2;
	}
}


void ODBCTest::testMultiMapComplex()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->multiMapComplex();
		i += 2;
	}
}


void ODBCTest::testSelectIntoSingle()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->selectIntoSingle();
		i += 2;
	}
}


void ODBCTest::testSelectIntoSingleStep()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->selectIntoSingleStep();
		i += 2;
	}	
}


void ODBCTest::testSelectIntoSingleFail()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->selectIntoSingleFail();
		i += 2;
	}	
}


void ODBCTest::testLowerLimitOk()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->lowerLimitOk();
		i += 2;
	}	
}


void ODBCTest::testSingleSelect()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->singleSelect();
		i += 2;
	}	
}


void ODBCTest::testLowerLimitFail()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->lowerLimitFail();
		i += 2;
	}
}


void ODBCTest::testCombinedLimits()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->combinedLimits();
		i += 2;
	}
}



void ODBCTest::testRange()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->ranges();
		i += 2;
	}
}


void ODBCTest::testCombinedIllegalLimits()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->combinedIllegalLimits();
		i += 2;
	}
}



void ODBCTest::testIllegalRange()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->illegalRange();
		i += 2;
	}
}


void ODBCTest::testEmptyDB()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->emptyDB();
		i += 2;
	}
}


void ODBCTest::testBLOB()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonBLOBTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->blob();
		i += 2;
	}
}


void ODBCTest::testBLOBContainer()
{
	for (int i = 0; i < 8;)
	{
		session().setFeature("autoBind", bindValue(i));
		session().setFeature("autoExtract", bindValue(i+1));
		recreatePersonBLOBTable();
		_pExecutor->blobContainer<std::vector<std::string>, std::vector<CLOB> >(10);
		recreatePersonBLOBTable();
		_pExecutor->blobContainer<std::deque<std::string>, std::deque<CLOB> >(10);
		recreatePersonBLOBTable();
		_pExecutor->blobContainer<std::list<std::string>, std::list<CLOB> >(10);
		i += 2;
	}
}


void ODBCTest::testBLOBStmt()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonBLOBTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->blobStmt();
		i += 2;
	}
}


void ODBCTest::testDateTime()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonDateTimeTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->dateTime();
		i += 2;
	}
}


void ODBCTest::testDate()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonDateTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->date();
		i += 2;
	}
}


void ODBCTest::testTime()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTimeTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->time();
		i += 2;
	}
}


void ODBCTest::testFloat()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateFloatsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->floats();
		i += 2;
	}
}


void ODBCTest::testDouble()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateFloatsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->doubles();
		i += 2;
	}
}


void ODBCTest::testTuple()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateTuplesTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->tuples();
		i += 2;
	}
}


void ODBCTest::testTupleVector()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateTuplesTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->tupleVector();
		i += 2;
	}
}


void ODBCTest::testInternalExtraction()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateVectorsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->internalExtraction();
		i += 2;
	}
}


void ODBCTest::testFilter()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateVectorsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->filter();
		i += 2;
	}
}


void ODBCTest::testInternalBulkExtraction()
{
	if (!_pSession) fail ("Test not available.");

	recreatePersonTable();
	_pSession->setFeature("autoBind", true);
	_pSession->setFeature("autoExtract", true);
#ifdef POCO_ODBC_UNICODE
	_pExecutor->internalBulkExtractionUTF16();
#else
	_pExecutor->internalBulkExtraction();
#endif
}


void ODBCTest::testInternalStorageType()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateVectorsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->internalStorageType();
		i += 2;
	}
}


void ODBCTest::testNull()
{
	if (!_pSession) fail ("Test not available.");

	// test for NOT NULL violation exception
	for (int i = 0; i < 8;)
	{
		recreateNullsTable("NOT NULL");
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->notNulls();
		i += 2;
	}

	// test for null insertion
	for (int i = 0; i < 8;)
	{
		recreateNullsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->nulls();
		i += 2;
	}
}


void ODBCTest::testRowIterator()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateVectorsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->rowIterator();
		i += 2;
	}
}


void ODBCTest::testStdVectorBool()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateBoolTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->stdVectorBool();
		i += 2;
	}
}


void ODBCTest::testAsync()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateIntsTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->asynchronous(2000);
		i += 2;
	}
}


void ODBCTest::testAny()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateAnysTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->any();

		i += 2;
	}
}


void ODBCTest::testDynamicAny()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateAnysTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->dynamicAny();

		i += 2;
	}
}


void ODBCTest::testMultipleResults()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->multipleResults();

		i += 2;
	}
}


void ODBCTest::testSQLChannel()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateLogTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->sqlChannel(_rConnectString);

		i += 2;
	}
}


void ODBCTest::testSQLLogger()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateLogTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->sqlLogger(_rConnectString);

		i += 2;
	}
}


void ODBCTest::testSessionTransaction()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->sessionTransaction(_rConnectString);
		i += 2;
	}
}


void ODBCTest::testTransaction()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->transaction(_rConnectString);
		i += 2;
	}
}


void ODBCTest::testTransactor()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->transactor();
		i += 2;
	}
}


void ODBCTest::testNullable()
{
	if (!_pSession) fail ("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateNullableTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->nullable();
		i += 2;
	}
}


void ODBCTest::testUnicode()
{
#if defined (POCO_ODBC_UNICODE)
	if (!_pSession) fail("Test not available.");

	for (int i = 0; i < 8;)
	{
		recreateUnicodeTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i + 1));
		_pExecutor->unicode(_rConnectString);
		i += 2;
	}
#else
	std::cout << "Not an UNICODE build, skipping." << std::endl;
#endif
}


void ODBCTest::testReconnect()
{
	if (!_pSession) fail ("Test not available.");

	std::string tableName("Person");

	for (int i = 0; i < 8;)
	{
		recreatePersonTable();
		_pSession->setFeature("autoBind", bindValue(i));
		_pSession->setFeature("autoExtract", bindValue(i+1));
		_pExecutor->reconnect();
		i += 2;
	}
}


bool ODBCTest::canConnect(const std::string& driver,
	std::string& dsn,
	std::string& uid,
	std::string& pwd,
	std::string& dbConnString,
	const std::string& db)
{
	Utility::DriverMap::iterator itDrv = _drivers.begin();
	for (; itDrv != _drivers.end(); ++itDrv)
	{
		if (((itDrv->first).find(driver) != std::string::npos))
		{
			std::cout << "Driver found: " << itDrv->first 
				<< " (" << itDrv->second << ')' << std::endl;
			break;
		}
	}

	if (_drivers.end() == itDrv) 
	{
		dsn = "";
		uid = "";
		pwd = "";
		dbConnString = "";
		std::cout << driver << " driver NOT found, tests not available." << std::endl;
		return false;
	}

	Utility::DSNMap dataSources;
	Utility::dataSources(dataSources);
	if (dataSources.size() > 0)
	{
		Utility::DSNMap::iterator itDSN = dataSources.begin();
		std::cout << dataSources.size() << " DSNs found, enumerating ..." << std::endl;
		for (; itDSN != dataSources.end(); ++itDSN)
		{
			if (itDSN->first == dsn && itDSN->second == driver)
			{
				std::cout << "DSN found: " << itDSN->first
					<< " (" << itDSN->second << ')' << std::endl;

				dbConnString = format("DSN=%s;UID=%s;PWD=%s;", dsn, uid, pwd);
				if (!db.empty())
					format(dbConnString, "DATABASE=%s;", db);

				return true;
			}
		}
	}
	else
		std::cout << "No DSNs found, will attempt DSN-less connection ..." << std::endl;

	dsn = "";
	return true;
}


void ODBCTest::setUp()
{
}


void ODBCTest::tearDown()
{
}


ODBCTest::SessionPtr ODBCTest::init(const std::string& driver,
	std::string& dsn,
	std::string& uid,
	std::string& pwd,
	std::string& dbConnString,
	const std::string& db)
{
	Utility::drivers(_drivers);
	if (!canConnect(driver, dsn, uid, pwd, dbConnString, db)) return 0;
	
	try
	{
		std::cout << "Conecting to [" << dbConnString << ']' << std::endl;
		return new Session(Poco::Data::ODBC::Connector::KEY, dbConnString, 5);
	}catch (ConnectionFailedException& ex)
	{
		std::cout << ex.displayText() << std::endl;
		return 0;
	}
}
